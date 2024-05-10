from lib import PgConnect


class SchemaDdl:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_schema(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.srv_wf_settings(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    courier_id varchar NOT NULL,
    courier_name varchar NOT NULL,

    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL
);

CREATE INDEX IF NOT EXISTS IDX_dm_couriers__courier_id ON dds.dm_couriers (courier_id);

CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    order_id int NOT NULL references dds.dm_orders(id),
    delivery_id VARCHAR NOT NULL unique,
    courier_id int NOT NULL references dds.dm_couriers(id),
    timestamp_id int NOT NULL references dds.dm_timestamps(id),
    rate smallint not null check(rate between 1 and 5),
    tip_sum numeric(14, 5) NOT NULL DEFAULT 0 CHECK (tip_sum >= 0),
    order_sum numeric(14, 5) NOT NULL DEFAULT 0 CHECK (order_sum >= 0)
);
CREATE INDEX IF NOT EXISTS idx_fct_deliveries__order_id ON dds.fct_deliveries USING btree (order_id);
CREATE INDEX IF NOT EXISTS idx_fct_deliveries__order_id ON dds.fct_deliveries USING btree (delivery_id);
CREATE INDEX IF NOT EXISTS idx_fct_deliveries__order_id ON dds.fct_deliveries USING btree (courier_id);

ALTER TABLE dds.dm_orders ADD COLUMN IF NOT EXISTS courier_id int REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.dm_orders DROP CONSTRAINT IF EXISTS dm_orders_courier_id_fkey;
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
"""
                )
