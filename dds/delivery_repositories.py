from datetime import datetime

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from typing import List, Optional


class DeliveryStgObj(BaseModel):
    id: int
    order_id: str
    delivery_id: str
    courier_id: str
    delivery_json: dict
    order_ts: datetime
    delivery_ts: datetime
    update_ts: datetime


class DeliveryDdsObj(BaseModel):
    id: int
    order_id: int
    delivery_id: str
    courier_id: int
    timestamp_id: int
    rate: int
    tip_sum: float
    order_sum: float


class DeliveryStgRepository:
    def load_stg_deliveries(self, conn: Connection, last_loaded_ts: int) -> List[DeliveryStgObj]:
        with conn.cursor(row_factory=class_row(DeliveryStgObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_id,
                        delivery_id,
                        courier_id,
                        delivery_json,
                        order_ts,
                        delivery_ts,
                        update_ts
                    FROM stg.deliverysystem_deliveries
                    WHERE update_ts > %(last_loaded_ts)s
                    ORDER BY update_ts ASC;
                """,
                {"last_loaded_ts": last_loaded_ts},
            )
            objs = cur.fetchall()
        objs.sort(key=lambda x: x.id)
        return objs


class DeliveryDdsRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(delivery_id, order_id, timestamp_id, courier_id, rate, tip_sum, order_sum)
                    VALUES (%(delivery_id)s, %(order_id)s, %(timestamp_id)s, %(courier_id)s, %(rate)s, %(tip_sum)s, %(order_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        courier_id = EXCLUDED.courier_id,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum,
                        order_sum = EXCLUDED.order_sum
                    ;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "order_id": delivery.order_id,
                    "timestamp_id": delivery.timestamp_id,
                    "courier_id": delivery.courier_id,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum,
                    "order_sum": delivery.order_sum
                },
            )

    def get_delivery(self, conn: Connection, delivery_id: str) -> Optional[DeliveryDdsObj]:
        with conn.cursor(row_factory=class_row(DeliveryDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_id,
                        delivery_id,
                        courier_id,
                        timestamp_id,
                        rate,
                        tip_sum,
                        order_sum,
                    FROM dds.fct_deliveries
                    WHERE delivery_id = %(delivery_id)s;
                """,
                {"delivery_id": delivery_id},
            )
            obj = cur.fetchone()
        return obj

