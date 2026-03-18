-- ============================================================
-- Warehouse Layer: Star Schema
-- dim_customer, dim_product, dim_region, dim_date
-- fact_sales, agg_sales_summary
-- ============================================================

-- dim_customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id   TEXT PRIMARY KEY,
    customer_name TEXT,
    region        TEXT,
    updated_at    TIMESTAMP DEFAULT NOW()
);

-- dim_product
CREATE TABLE IF NOT EXISTS dim_product (
    product_id   TEXT PRIMARY KEY,
    product_name TEXT,
    category     TEXT,
    sub_category TEXT,
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- dim_region
CREATE TABLE IF NOT EXISTS dim_region (
    region_id   SERIAL PRIMARY KEY,
    region_name TEXT UNIQUE NOT NULL,
    updated_at  TIMESTAMP DEFAULT NOW()
);

-- dim_date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id      DATE PRIMARY KEY,
    day          INT,
    month        INT,
    year         INT,
    quarter      INT,
    day_of_week  INT,
    week_of_year INT,
    is_weekend   BOOLEAN
);

-- fact_sales
CREATE TABLE IF NOT EXISTS fact_sales (
    id            SERIAL PRIMARY KEY,
    order_id      TEXT NOT NULL,
    customer_id   TEXT REFERENCES dim_customer(customer_id),
    product_id    TEXT REFERENCES dim_product(product_id),
    order_date    DATE REFERENCES dim_date(date_id),
    ship_date     DATE,
    region        TEXT,
    sales         FLOAT,
    quantity      INT,
    discount      FLOAT,
    profit        FLOAT,
    profit_margin FLOAT,
    order_month   INT,
    order_year    INT,
    loaded_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date     ON fact_sales (order_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product  ON fact_sales (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_region   ON fact_sales (region);

-- agg_sales_summary
CREATE TABLE IF NOT EXISTS agg_sales_summary (
    id            SERIAL PRIMARY KEY,
    agg_date      DATE,
    region        TEXT,
    category      TEXT,
    total_sales   FLOAT,
    total_profit  FLOAT,
    total_orders  INT,
    avg_discount  FLOAT,
    refreshed_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE (agg_date, region, category)
);
