-- ============================================================
-- 02_create_warehouse.sql
-- Data Warehouse: Star Schema (Fact + Dimension tables)
-- ============================================================

-- ─── Dimension: Customer ────────────────────────────────────
DROP TABLE IF EXISTS dim_customer CASCADE;
CREATE TABLE dim_customer (
    customer_sk     SERIAL PRIMARY KEY,
    customer_id     TEXT UNIQUE NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ─── Dimension: Product ─────────────────────────────────────
DROP TABLE IF EXISTS dim_product CASCADE;
CREATE TABLE dim_product (
    product_sk      SERIAL PRIMARY KEY,
    product_id      TEXT UNIQUE NOT NULL,
    category        TEXT,
    sub_category    TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ─── Dimension: Region ──────────────────────────────────────
DROP TABLE IF EXISTS dim_region CASCADE;
CREATE TABLE dim_region (
    region_sk       SERIAL PRIMARY KEY,
    region          TEXT NOT NULL,
    state           TEXT,
    UNIQUE(region, state)
);

-- ─── Dimension: Date ────────────────────────────────────────
DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
    date_sk         INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date       DATE UNIQUE NOT NULL,
    day_of_week     INTEGER,               -- 0=Mon … 6=Sun
    day_name        TEXT,
    week_of_year    INTEGER,
    month           INTEGER,
    month_name      TEXT,
    quarter         INTEGER,
    year            INTEGER,
    is_weekend      BOOLEAN
);

-- ─── Fact: Sales ────────────────────────────────────────────
DROP TABLE IF EXISTS fact_sales CASCADE;
CREATE TABLE fact_sales (
    sale_sk         SERIAL PRIMARY KEY,
    order_id        TEXT NOT NULL,
    customer_sk     INTEGER REFERENCES dim_customer(customer_sk),
    product_sk      INTEGER REFERENCES dim_product(product_sk),
    region_sk       INTEGER REFERENCES dim_region(region_sk),
    date_sk         INTEGER REFERENCES dim_date(date_sk),
    order_date      DATE,
    ship_date       DATE,
    sales           FLOAT,
    quantity        INTEGER,
    discount        FLOAT,
    profit          FLOAT,
    profit_margin   FLOAT,
    order_month     INTEGER,
    order_year      INTEGER,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_fact_order_id    ON fact_sales(order_id);
CREATE INDEX idx_fact_date_sk     ON fact_sales(date_sk);
CREATE INDEX idx_fact_customer_sk ON fact_sales(customer_sk);
CREATE INDEX idx_fact_product_sk  ON fact_sales(product_sk);
CREATE INDEX idx_fact_region_sk   ON fact_sales(region_sk);
CREATE INDEX idx_fact_order_date  ON fact_sales(order_date);

-- ─── Aggregate: Sales Summary ───────────────────────────────
DROP TABLE IF EXISTS agg_sales_summary CASCADE;
CREATE TABLE agg_sales_summary (
    agg_sk          SERIAL PRIMARY KEY,
    agg_date        DATE NOT NULL,
    region          TEXT NOT NULL,
    category        TEXT NOT NULL,
    total_sales     FLOAT,
    total_profit    FLOAT,
    total_orders    INTEGER,
    avg_discount    FLOAT,
    profit_margin   FLOAT,
    refreshed_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE(agg_date, region, category)
);

CREATE INDEX idx_agg_date     ON agg_sales_summary(agg_date);
CREATE INDEX idx_agg_region   ON agg_sales_summary(region);
CREATE INDEX idx_agg_category ON agg_sales_summary(category);
