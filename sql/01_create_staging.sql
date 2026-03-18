-- ============================================================
-- Staging Layer: stg_superstore
-- Raw data loaded from superstore.csv before transformation
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_superstore (
    order_id      TEXT,
    order_date    DATE,
    ship_date     DATE,
    customer_id   TEXT,
    customer_name TEXT,
    region        TEXT,
    product_id    TEXT,
    product_name  TEXT,
    category      TEXT,
    sub_category  TEXT,
    sales         FLOAT,
    quantity      INT,
    discount      FLOAT,
    profit        FLOAT,
    loaded_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_order_product
    ON stg_superstore (order_id, product_id);
