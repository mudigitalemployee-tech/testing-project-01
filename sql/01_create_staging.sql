-- ============================================================
-- 01_create_staging.sql
-- Staging layer: raw ingestion table
-- ============================================================

DROP TABLE IF EXISTS stg_superstore;

CREATE TABLE stg_superstore (
    row_id          SERIAL PRIMARY KEY,
    order_id        TEXT,
    order_date      DATE,
    ship_date       DATE,
    customer_id     TEXT,
    region          TEXT,
    state           TEXT,
    category        TEXT,
    sub_category    TEXT,
    product_id      TEXT,
    sales           FLOAT,
    quantity        INTEGER,
    discount        FLOAT,
    profit          FLOAT,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_order_id    ON stg_superstore(order_id);
CREATE INDEX idx_stg_order_date  ON stg_superstore(order_date);
CREATE INDEX idx_stg_customer_id ON stg_superstore(customer_id);
CREATE INDEX idx_stg_region      ON stg_superstore(region);
