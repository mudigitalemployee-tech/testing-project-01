"""
transform.py — Phase 3: Transform & Load
Reads stg_superstore → cleans → builds star schema → loads warehouse.
"""

import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TRANSFORM] %(message)s")
log = logging.getLogger(__name__)

DB_CONN = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "salesdb"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
DAYS   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]


# ── Helpers ─────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONN)


def read_staging(conn) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM stg_superstore", conn)
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["ship_date"]  = pd.to_datetime(df["ship_date"])
    log.info(f"Read {len(df)} rows from staging")
    return df


# ── Cleaning ────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    original = len(df)

    # Drop nulls in critical columns
    df = df.dropna(subset=["sales", "profit"])

    # Default discount = 0
    df["discount"] = df["discount"].fillna(0.0)

    # Remove duplicates on order_id + product_id (keep first)
    df = df.drop_duplicates(subset=["order_id", "product_id"], keep="first")

    # Derived columns
    df["order_month"]    = df["order_date"].dt.month
    df["order_year"]     = df["order_date"].dt.year
    df["profit_margin"]  = df.apply(
        lambda r: round(r["profit"] / r["sales"], 4) if r["sales"] != 0 else 0.0, axis=1
    )

    log.info(f"Cleaned: {original} → {len(df)} rows | +order_month, +order_year, +profit_margin")
    return df


# ── Dimension Loaders ────────────────────────────────────────

def load_dim_customer(df: pd.DataFrame, conn) -> dict:
    customers = df["customer_id"].dropna().unique().tolist()
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO dim_customer (customer_id) VALUES %s ON CONFLICT (customer_id) DO NOTHING",
            [(c,) for c in customers]
        )
        cur.execute("SELECT customer_sk, customer_id FROM dim_customer")
        mapping = {row[1]: row[0] for row in cur.fetchall()}
    log.info(f"dim_customer: {len(mapping)} records")
    return mapping


def load_dim_product(df: pd.DataFrame, conn) -> dict:
    products = df[["product_id","category","sub_category"]].drop_duplicates("product_id")
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO dim_product (product_id, category, sub_category)
               VALUES %s ON CONFLICT (product_id) DO NOTHING""",
            [(r["product_id"], r["category"], r["sub_category"]) for _, r in products.iterrows()]
        )
        cur.execute("SELECT product_sk, product_id FROM dim_product")
        mapping = {row[1]: row[0] for row in cur.fetchall()}
    log.info(f"dim_product: {len(mapping)} records")
    return mapping


def load_dim_region(df: pd.DataFrame, conn) -> dict:
    regions = df[["region","state"]].drop_duplicates()
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO dim_region (region, state)
               VALUES %s ON CONFLICT (region, state) DO NOTHING""",
            [(r["region"], r["state"]) for _, r in regions.iterrows()]
        )
        cur.execute("SELECT region_sk, region, state FROM dim_region")
        mapping = {(row[1], row[2]): row[0] for row in cur.fetchall()}
    log.info(f"dim_region: {len(mapping)} records")
    return mapping


def load_dim_date(df: pd.DataFrame, conn) -> dict:
    all_dates = pd.concat([df["order_date"], df["ship_date"]]).dt.date.unique()
    with conn.cursor() as cur:
        records = []
        for d in all_dates:
            dt = pd.Timestamp(d)
            date_sk = int(dt.strftime("%Y%m%d"))
            records.append((
                date_sk, d,
                dt.dayofweek, DAYS[dt.dayofweek],
                dt.isocalendar().week,
                dt.month, MONTHS[dt.month - 1],
                dt.quarter, dt.year,
                dt.dayofweek >= 5
            ))
        execute_values(
            cur,
            """INSERT INTO dim_date
               (date_sk, full_date, day_of_week, day_name, week_of_year,
                month, month_name, quarter, year, is_weekend)
               VALUES %s ON CONFLICT (date_sk) DO NOTHING""",
            records
        )
        cur.execute("SELECT date_sk, full_date FROM dim_date")
        mapping = {str(row[1]): row[0] for row in cur.fetchall()}
    log.info(f"dim_date: {len(mapping)} records")
    return mapping


# ── Fact Loader ──────────────────────────────────────────────

def load_fact_sales(df: pd.DataFrame, conn, cust_map, prod_map, reg_map, date_map) -> int:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE fact_sales RESTART IDENTITY;")
        records = []
        for _, row in df.iterrows():
            cust_sk   = cust_map.get(row["customer_id"])
            prod_sk   = prod_map.get(row["product_id"])
            reg_sk    = reg_map.get((row["region"], row["state"]))
            date_sk   = date_map.get(str(row["order_date"].date()))
            records.append((
                row["order_id"],
                cust_sk, prod_sk, reg_sk, date_sk,
                row["order_date"].date(),
                row["ship_date"].date(),
                float(row["sales"]),
                int(row["quantity"]),
                float(row["discount"]),
                float(row["profit"]),
                float(row["profit_margin"]),
                int(row["order_month"]),
                int(row["order_year"]),
            ))
        execute_values(
            cur,
            """INSERT INTO fact_sales
               (order_id, customer_sk, product_sk, region_sk, date_sk,
                order_date, ship_date, sales, quantity, discount, profit,
                profit_margin, order_month, order_year)
               VALUES %s""",
            records, page_size=1000
        )
    log.info(f"fact_sales: {len(records)} records loaded")
    return len(records)


# ── Aggregate Builder ────────────────────────────────────────

def build_aggregates(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE agg_sales_summary RESTART IDENTITY;")
        cur.execute("""
            INSERT INTO agg_sales_summary
                (agg_date, region, category, total_sales, total_profit,
                 total_orders, avg_discount, profit_margin)
            SELECT
                fs.order_date                           AS agg_date,
                dr.region                               AS region,
                dp.category                             AS category,
                ROUND(SUM(fs.sales)::NUMERIC, 2)        AS total_sales,
                ROUND(SUM(fs.profit)::NUMERIC, 2)       AS total_profit,
                COUNT(DISTINCT fs.order_id)             AS total_orders,
                ROUND(AVG(fs.discount)::NUMERIC, 4)     AS avg_discount,
                CASE WHEN SUM(fs.sales) = 0 THEN 0
                     ELSE ROUND((SUM(fs.profit)/SUM(fs.sales))::NUMERIC, 4)
                END                                     AS profit_margin
            FROM fact_sales fs
            JOIN dim_region  dr ON fs.region_sk  = dr.region_sk
            JOIN dim_product dp ON fs.product_sk = dp.product_sk
            GROUP BY fs.order_date, dr.region, dp.category
            ON CONFLICT (agg_date, region, category) DO UPDATE SET
                total_sales   = EXCLUDED.total_sales,
                total_profit  = EXCLUDED.total_profit,
                total_orders  = EXCLUDED.total_orders,
                avg_discount  = EXCLUDED.avg_discount,
                profit_margin = EXCLUDED.profit_margin,
                refreshed_at  = NOW();
        """)
        cur.execute("SELECT COUNT(*) FROM agg_sales_summary;")
        count = cur.fetchone()[0]
    log.info(f"agg_sales_summary: {count} records built")
    return count


# ── Main ─────────────────────────────────────────────────────

def run():
    log.info("=== TRANSFORM PHASE START ===")
    conn = get_conn()
    try:
        with conn:
            df     = read_staging(conn)
            df     = clean(df)
            c_map  = load_dim_customer(df, conn)
            p_map  = load_dim_product(df, conn)
            r_map  = load_dim_region(df, conn)
            d_map  = load_dim_date(df, conn)
            n_fact = load_fact_sales(df, conn, c_map, p_map, r_map, d_map)
            n_agg  = build_aggregates(conn)
        log.info(f"=== TRANSFORM COMPLETE | fact_sales={n_fact} | agg_rows={n_agg} ===")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
