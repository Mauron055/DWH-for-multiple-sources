from lib import PgConnect


class SchemaDdl:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_schema(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.deliverysystem_restaurants (
    id SERIAL NOT NULL PRIMARY KEY,
    restaurant_id VARCHAR UNIQUE NOT NULL,
    restaurant_json JSONB NOT NULL,
    update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
    id SERIAL NOT NULL PRIMARY KEY,
    courier_id VARCHAR UNIQUE NOT NULL,
    courier_json JSONB NOT NULL,
    update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
    id SERIAL NOT NULL PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    delivery_id VARCHAR NOT NULL,
    courier_id VARCHAR NOT NULL,
    delivery_json JSONB NOT NULL,
    order_ts timestamp NOT NULL,
    delivery_ts timestamp NOT NULL,
    update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(order_id, delivery_id, courier_id)
);

"""
                )
