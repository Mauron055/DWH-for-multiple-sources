from lib import PgConnect


class SchemaDdl:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_schema(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year INT NOT NULL CHECK(settlement_year > 2022 AND settlement_year < 2500),
    settlement_month INT NOT NULL CHECK(settlement_month >= 1 AND settlement_month <= 12),
    orders_count INT NOT NULL CHECK(orders_count >= 0) DEFAULT 0,
    orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    rate_avg NUMERIC(14, 5) NOT NULL DEFAULT 0 CHECK (rate_avg >= 0),
    order_processing_fee NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    courier_order_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
    courier_tips_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
    courier_reward_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0),
    UNIQUE(courier_id, settlement_year, settlement_month)
);

"""
                )
