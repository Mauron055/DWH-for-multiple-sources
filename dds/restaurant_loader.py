import json

from datetime import datetime
from typing import List, Optional, Any

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


class RestaurantStgObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_json: dict
    update_ts: datetime


class RestaurantDdsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


class RestaurantStgRepository:
    def load_stg_restaurants(self, conn: Connection, last_loaded_record_ts: int) -> List[RestaurantStgObj]:
        with conn.cursor(row_factory=class_row(RestaurantStgObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        restaurant_id,
                        restaurant_json,
                        update_ts
                    FROM stg.deliverysystem_restaurants
                    WHERE update_ts > %(last_loaded_record_ts)s;
                """,
                {"last_loaded_record_ts": last_loaded_record_ts},
            )
            objs = cur.fetchall()
        return objs


class RestaurantDdsRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s);
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to
                },
            )

    def update_old_restaurants(self, conn: Connection, restaurant: RestaurantDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE dds.dm_restaurants
                    SET active_to = %(active_from)s
                    WHERE restaurant_id = %(restaurant_id)s
                        and active_to = %(active_to)s
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to
                },
            )

    def get_restaurant(self, conn: Connection, restaurant_id: str) -> Optional[RestaurantDdsObj]:
        with conn.cursor(row_factory=class_row(RestaurantDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        restaurant_id,
                        restaurant_name,
                        active_from,
                        active_to
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s
                    and  active_to = '2099-12-31'::timestamp  ;
                """,
                {"restaurant_id": restaurant_id},
            )
            obj = cur.fetchone()
        return obj


class RestaurantLoader:
    WF_KEY = "restaurants_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.stg = RestaurantStgRepository()
        self.dds = RestaurantDdsRepository()
        self.settings_repository = settings_repository

    def parse_restaurants(self, raws: List[RestaurantStgObj]) -> List[RestaurantDdsObj]:
        res = []
        for r in raws:
            
            t = RestaurantDdsObj(id=r.id,
                                 restaurant_id=r.restaurant_id,
                                 restaurant_name=r.restaurant_json['name'],
                                 active_from=r.update_ts,
                                 active_to=datetime(year=2099, month=12, day=31)
                                 )

            res.append(t)
        return res

    def load_restaurants(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    })

            last_loaded_ts = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY])
            
            load_queue = self.stg.load_stg_restaurants(conn, last_loaded_ts)
            
            if not load_queue:
                return 0
            
            last_loaded_ts = load_queue[-1].update_ts.isoformat()
            print(f'NEW checkpoint: {last_loaded_ts}')
            
            restaurants_to_load = self.parse_restaurants(load_queue)
            
            for r in restaurants_to_load:
                existing = self.dds.get_restaurant(conn, r.restaurant_id)
                if not existing:
                    self.dds.insert_restaurant(conn, r)
                else:
                    self.dds.update_old_restaurants(conn, r)
                    self.dds.insert_restaurant(conn, r)

                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded_ts
                self.settings_repository.save_setting(conn, wf_setting)
