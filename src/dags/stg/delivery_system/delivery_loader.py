import json
import logging

from datetime import datetime
from typing import List, Optional

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from stg.stg_settings_repository import StgEtlSettingsRepository, EtlSetting
from lib.delivery_system_api import DeliveriesAPI
from datetime import datetime, timedelta

class DeliveryJsonObj(BaseModel):
    object_value: str


class DeliveryStgObj(BaseModel):
    order_id: str
    delivery_id: str
    courier_id: str
    delivery_json: str
    order_ts: datetime
    delivery_ts: datetime
    update_ts: datetime


class DeliveryStgRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryStgObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(order_id, delivery_id, courier_id, 
                                                            delivery_json, order_ts, delivery_ts, update_ts)
                    VALUES (%(order_id)s, %(delivery_id)s, %(courier_id)s,
                            %(delivery_json)s, %(order_ts)s, %(delivery_ts)s, %(update_ts)s)
                    ON CONFLICT (order_id, delivery_id, courier_id) DO UPDATE
                    SET
                        delivery_json = EXCLUDED.delivery_json,
                        order_ts = EXCLUDED.order_ts,
                        delivery_ts = EXCLUDED.delivery_ts,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "order_id": delivery.order_id,
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "delivery_json": delivery.delivery_json,
                    "order_ts": delivery.order_ts,
                    "delivery_ts": delivery.delivery_ts,
                    "update_ts": delivery.update_ts
                },
            )

    def get_delivery(self, conn: Connection,
                     order_id: str, delivery_id: str, courier_id: str ) -> Optional[DeliveryStgObj]:
        with conn.cursor(row_factory=class_row(DeliveryStgObj)) as cur:
            cur.execute(
                """
                    SELECT
                        order_id,
                        delivery_id,
                        courier_id,
                        delivery_json,
                        order_ts,
                        delivery_ts,
                        update_ts,
                    FROM stg.deliverysystem_deliveries
                    WHERE order_id = %(order_id)s
                        AND delivery_id = %(delivery_id)s
                        AND courier_id = %(courier_id)s;
                """,
                {"order_id": order_id, "delivery_id": delivery_id, "courier_id": courier_id},
            )
            obj = cur.fetchone()
        return obj


class DeliveryLoader:
    WF_KEY = "deliveries_api_to_stg_workflow"
    LAST_LOADED_OFFSET_KEY = "last_loaded_offset"

    def __init__(self, source_api: DeliveriesAPI, pg: PgConnect, settings_repository: StgEtlSettingsRepository) -> None:
        self.dwh = pg
        self.source_api = source_api
        self.stg = DeliveryStgRepository()
        self.settings_repository = settings_repository

    def parse_deliverys(self, raws: List[DeliveryJsonObj]) -> List[DeliveryStgObj]:
        res = []
        for r in raws:
            t = DeliveryStgObj(
                                order_id=r['order_id'],
                                delivery_id=r['delivery_id'],
                                courier_id=r['courier_id'],
                                delivery_json=str(r).replace("'",'"'),
                                order_ts=r['order_ts'],
                                delivery_ts=r['delivery_ts'],
                                update_ts=datetime.today()
                                )

            res.append(t)
        return res

    def load(self, last_date: str):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_OFFSET_KEY: 0})

            last_loaded_offset = wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY]
            logging.info(f"starting to load from last checkpoint: {last_loaded_offset}")

            to_dt = datetime.strptime(f"{last_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
            from_dt = to_dt - timedelta(days=7)
            params = {'offset': last_loaded_offset, 'limit': 50,
                   'sort_field': 'date',  'sort_direction': 'asc',
                   'from': from_dt, 'to': to_dt}
            load_queue = self.source_api.get(params)
            queue_len = len(load_queue)
            logging.info(f"get {queue_len} deliveries")
            
            if not load_queue:
                logging.info("Quitting.")
                return 0
            
            deliverys_to_load = self.parse_deliverys(load_queue)
            for r in deliverys_to_load:
                self.stg.insert_delivery(conn, r)

            wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY] = last_loaded_offset + queue_len
            self.settings_repository.save_setting(conn, wf_setting)
