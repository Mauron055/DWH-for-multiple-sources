import json
import logging

from datetime import datetime
from typing import List, Optional, Any

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


class CourierStgObj(BaseModel):
    id: int
    courier_id: str
    courier_json: dict
    update_ts: datetime


class CourierDdsObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str
    active_from: datetime
    active_to: datetime


class CourierStgRepository:
    def load_stg_couriers(self, conn: Connection, last_loaded_record_ts: int) -> List[CourierStgObj]:
        with conn.cursor(row_factory=class_row(CourierStgObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        courier_id,
                        courier_json,
                        update_ts
                    FROM stg.deliverysystem_couriers
                    WHERE update_ts > %(last_loaded_record_ts)s;
                """,
                {"last_loaded_record_ts": last_loaded_record_ts},
            )
            objs = cur.fetchall()
        return objs


class CourierDdsRepository:
    def insert_courier(self, conn: Connection, courier: CourierDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name, active_from, active_to)
                    VALUES (%(courier_id)s, %(courier_name)s, %(active_from)s, %(active_to)s);
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name,
                    "active_from": courier.active_from,
                    "active_to": courier.active_to
                },
            )

    def update_old_couriers(self, conn: Connection, courier: CourierDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE dds.dm_couriers
                    SET active_to = %(active_from)s
                    WHERE courier_id = %(courier_id)s
                        and active_to = %(active_to)s
                """,
                {
                    "courier_id": courier.courier_id,
                    "active_from": courier.active_from,
                    "active_to": courier.active_to
                },
            )

    def get_courier(self, conn: Connection, courier_id: str) -> Optional[CourierDdsObj]:
        
        with conn.cursor(row_factory=class_row(CourierDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        courier_id,
                        courier_name,
                        active_from,
                        active_to
                    FROM dds.dm_couriers
                    WHERE courier_id = %(courier_id)s
                    and  active_to = '2099-12-31'::timestamp ;
                """,
                {"courier_id": courier_id},
            )
            obj = cur.fetchone()
        return obj


class CourierLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.stg = CourierStgRepository()
        self.dds = CourierDdsRepository()
        self.settings_repository = settings_repository

    def parse_couriers(self, raws: List[CourierStgObj]) -> List[CourierDdsObj]:
        res = []
        
        for r in raws:
            
            t = CourierDdsObj(id=r.id,
                                 courier_id=r.courier_id,
                                 courier_name=r.courier_json['name'],
                                 active_from=r.update_ts,
                                 active_to=datetime(year=2099, month=12, day=31)
                                 )

            res.append(t)
        return res

    def load_couriers(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    })

            last_loaded_ts = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY])
            logging.info(f"starting to load from last checkpoint: {last_loaded_ts}")
            
            load_queue = self.stg.load_stg_couriers(conn, last_loaded_ts)
            logging.info(f"get {len(load_queue)} couriers")
            
            if not load_queue:
                logging.info("Quitting.")
                return 0
            
            last_loaded_ts = load_queue[-1].update_ts.isoformat()
            logging.info(f'NEW checkpoint: {last_loaded_ts}')
            
            couriers_to_load = self.parse_couriers(load_queue)
            
            for r in couriers_to_load:
                existing = self.dds.get_courier(conn, r.courier_id)
                if not existing:
                    self.dds.insert_courier(conn, r)
                else:
                    self.dds.update_old_couriers(conn, r)
                    self.dds.insert_courier(conn, r)

                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded_ts
                self.settings_repository.save_setting(conn, wf_setting)
