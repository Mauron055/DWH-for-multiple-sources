--слой cdm
CREATE TABLE cdm.dm_courier_ledger (
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

--слой dds
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

ALTER TABLE dds.dm_orders ADD COLUMN courier_id int REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.dm_orders DROP CONSTRAINT IF EXISTS dm_orders_courier_id_fkey;
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);


--слой stg
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
