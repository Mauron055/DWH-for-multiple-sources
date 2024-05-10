import json
import logging

from datetime import datetime
from typing import List, Optional

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from stg.stg_settings_repository import StgEtlSettingsRepository, EtlSetting
from lib.delivery_system_api import CouriersAPI


class CourierJsonObj(BaseModel):
    object_value: str


class CourierStgObj(BaseModel):
    courier_id: str
    courier_json: str
    update_ts: datetime


class CourierStgRepository:
    def insert_courier(self, conn: Connection, courier: CourierStgObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(courier_id, courier_json, update_ts)
                    VALUES (%(courier_id)s, %(courier_json)s, %(update_ts)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_json = EXCLUDED.courier_json,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_json": courier.courier_json,
                    "update_ts": courier.update_ts
                },
            )

    def get_courier(self, conn: Connection, courier_id: str) -> Optional[CourierStgObj]:
        with conn.cursor(row_factory=class_row(CourierStgObj)) as cur:
            cur.execute(
                """
                    SELECT
                        courier_id,
                        courier_json,
                        update_ts
                    FROM stg.deliverysystem_couriers
                    WHERE courier_id = %(courier_id)s;
                """,
                {"courier_id": courier_id},
            )
            obj = cur.fetchone()
        return obj


class CourierLoader:
    WF_KEY = "couriers_api_to_stg_workflow"
    LAST_LOADED_OFFSET_KEY = "last_loaded_offset"

    def __init__(self, source_api: CouriersAPI, pg: PgConnect, settings_repository: StgEtlSettingsRepository) -> None:
        self.dwh = pg
        self.source_api = source_api
        self.stg = CourierStgRepository()
        self.settings_repository = settings_repository

    def parse_couriers(self, raws: List[CourierJsonObj]) -> List[CourierStgObj]:
        res = []

        for r in raws:
            t = CourierStgObj(
                                 courier_id=r['_id'],
                                 courier_json=str(r).replace("'", '"'),
                                 update_ts=datetime.today()
                                 )

            res.append(t)
        return res

    def load(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_OFFSET_KEY: 0})

            last_loaded_offset = wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY]
            logging.info(f"starting to load from last checkpoint: {last_loaded_offset}")


            params = {'offset': last_loaded_offset, 'limit': 50,
                   'sort_field': '_id',  'sort_direction': 'asc'}
            load_queue = self.source_api.get(params)
            queue_len = len(load_queue)
            logging.info(f"get {queue_len} couriers")
            
            if not load_queue:
                logging.info("Quitting.")
                return 0
            
            couriers_to_load = self.parse_couriers(load_queue)
            for r in couriers_to_load:
                self.stg.insert_courier(conn, r)

            wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY] = last_loaded_offset + queue_len
            self.settings_repository.save_setting(conn, wf_setting)
