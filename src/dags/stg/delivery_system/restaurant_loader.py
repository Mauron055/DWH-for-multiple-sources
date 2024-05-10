import json
import logging

from datetime import datetime
from typing import List, Optional

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from stg.stg_settings_repository import StgEtlSettingsRepository, EtlSetting
from lib.delivery_system_api import RestaurantsAPI


class RestaurantJsonObj(BaseModel):
    object_value: str


class RestaurantStgObj(BaseModel):
    restaurant_id: str
    restaurant_json: str
    update_ts: datetime


class RestaurantStgRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantStgObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_restaurants(restaurant_id, restaurant_json, update_ts)
                    VALUES (%(restaurant_id)s, %(restaurant_json)s, %(update_ts)s)
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET
                        restaurant_json = EXCLUDED.restaurant_json,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_json": restaurant.restaurant_json,
                    "update_ts": restaurant.update_ts
                },
            )

    def get_restaurant(self, conn: Connection, restaurant_id: str) -> Optional[RestaurantStgObj]:
        with conn.cursor(row_factory=class_row(RestaurantStgObj)) as cur:
            cur.execute(
                """
                    SELECT
                        restaurant_id,
                        restaurant_json,
                        update_ts
                    FROM stg.deliverysystem_restaurants
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {"restaurant_id": restaurant_id},
            )
            obj = cur.fetchone()
        return obj


class RestaurantLoader:
    WF_KEY = "restaurants_api_to_stg_workflow"
    LAST_LOADED_OFFSET_KEY = "last_loaded_offset"

    def __init__(self, source_api: RestaurantsAPI, pg: PgConnect, settings_repository: StgEtlSettingsRepository) -> None:
        self.dwh = pg
        self.source_api = source_api
        self.stg = RestaurantStgRepository()
        self.settings_repository = settings_repository

    def parse_restaurants(self, raws: List[RestaurantJsonObj]) -> List[RestaurantStgObj]:
        res = []
        for r in raws:
            t = RestaurantStgObj(
                                 restaurant_id=r['_id'],
                                 restaurant_json=str(r).replace("'",'"'),
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
            logging.info(f"get {queue_len} restaurants")
            
            if not load_queue:
                logging.info("Quitting.")
                return 0
            
            restaurants_to_load = self.parse_restaurants(load_queue)
            for r in restaurants_to_load:
                self.stg.insert_restaurant(conn, r)

            wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY] = queue_len
            self.settings_repository.save_setting(conn, wf_setting)
