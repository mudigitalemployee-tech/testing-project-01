"""
Step 3 — Transformation Layer (PRD 6.3 & 6.4)
- Remove duplicates on (order_id, product_id)
- Drop rows where sales or profit is null
- Fill missing discount with 0
- Derive order_month, order_year, profit_margin
- Load star schema: dim_date, dim_customer, dim_product, dim_region, fact_sales
"""
import os, sys
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import get_connection, load_config, execute_sql, execute_sql_file, get_logger

logger = get_logger(__name__)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    df = df.dropna(subset=["sales", "profit"])
    df["discount"] = df["discount"].fillna(0.0)
    df = df.drop_duplicates(subset=["order_id", "product_id"], keep="first")
    df["order_date"]    = pd.to_datetime(df["order_date"], errors="coerce")
    df["order_month"]   = df["order_date"].dt.month
    df["order_year"]    = df["order_date"].dt.year
    df["profit_margin"] = df.apply(
        lambda r: round(r["profit"] / r["sales"], 4) if r["sales"] != 0 else 0.0, axis=1)
    logger.info(f"Clean: {n} → {len(df)} rows")
    return df


def load_dim_date(conn, df):
    dates = pd.concat([
        df["order_date"].dropna(),
        pd.to_datetime(df.get("ship_date", pd.Series(dtype="object")), errors="coerce").dropna()
    ]).unique()
    with conn.cursor() as cur:
        for d in dates:
            d = pd.Timestamp(d).date()
            cur.execute("""
                INSERT INTO dim_date (date_id,day,month,year,quarter,day_of_week,week_of_year,is_weekend)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (date_id) DO NOTHING;
            """, (d, d.day, d.month, d.year, (d.month-1)//3+1,
                  d.weekday(), d.isocalendar()[1], d.weekday() >= 5))
    conn.commit()
    logger.info(f"dim_date: {len(dates)} rows.")


def load_dim_customer(conn, df):
    customers = df[["customer_id","customer_name","region"]].drop_duplicates("customer_id")
    with conn.cursor() as cur:
        for _, r in customers.iterrows():
            cur.execute("""
                INSERT INTO dim_customer (customer_id,customer_name,region)
                VALUES (%s,%s,%s)
                ON CONFLICT (customer_id) DO UPDATE
                SET customer_name=EXCLUDED.customer_name, region=EXCLUDED.region, updated_at=NOW();
            """, (r["customer_id"], r.get("customer_name"), r["region"]))
    conn.commit()
    logger.info(f"dim_customer: {len(customers)} rows.")


def load_dim_product(conn, df):
    products = df[["product_id","product_name","category","sub_category"]].drop_duplicates("product_id")
    with conn.cursor() as cur:
        for _, r in products.iterrows():
            cur.execute("""
                INSERT INTO dim_product (product_id,product_name,category,sub_category)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (product_id) DO UPDATE
                SET product_name=EXCLUDED.product_name, category=EXCLUDED.category,
                    sub_category=EXCLUDED.sub_category, updated_at=NOW();
            """, (r["product_id"], r.get("product_name"), r.get("category"), r.get("sub_category")))
    conn.commit()
    logger.info(f"dim_product: {len(products)} rows.")


def load_dim_region(conn, df):
    for region in df["region"].dropna().unique():
        execute_sql(conn, "INSERT INTO dim_region (region_name) VALUES (%s) ON CONFLICT (region_name) DO NOTHING;", (region,))
    logger.info("dim_region loaded.")


def load_fact_sales(conn, df):
    fact_cols = ["order_id","customer_id","product_id","order_date","ship_date","region",
                 "sales","quantity","discount","profit","profit_margin","order_month","order_year"]
    df_f = df[[c for c in fact_cols if c in df.columns]].copy()
    df_f["order_date"] = df_f["order_date"].dt.date
    if "ship_date" in df_f.columns:
        df_f["ship_date"] = pd.to_datetime(df_f["ship_date"], errors="coerce").dt.date
    with conn.cursor() as cur:
        for _, r in df_f.iterrows():
            cur.execute("""
                INSERT INTO fact_sales
                    (order_id,customer_id,product_id,order_date,ship_date,region,
                     sales,quantity,discount,profit,profit_margin,order_month,order_year)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (order_id,product_id) DO UPDATE
                SET sales=EXCLUDED.sales, profit=EXCLUDED.profit,
                    discount=EXCLUDED.discount, profit_margin=EXCLUDED.profit_margin, loaded_at=NOW();
            """, (r["order_id"],r["customer_id"],r["product_id"],r["order_date"],
                  r.get("ship_date"),r["region"],float(r["sales"]),int(r["quantity"]),
                  float(r["discount"]),float(r["profit"]),float(r["profit_margin"]),
                  int(r["order_month"]),int(r["order_year"])))
    conn.commit()
    logger.info(f"fact_sales: {len(df_f)} rows upserted.")


def run(config_path=None):
    config = load_config(config_path)
    conn   = get_connection(config)
    try:
        sql_dir = os.path.join(os.path.dirname(__file__), "..", "sql")
        execute_sql_file(conn, os.path.join(sql_dir, "02_create_warehouse.sql"))
        rows = execute_sql(conn, "SELECT * FROM stg_superstore;", fetch=True)
        df   = pd.DataFrame([dict(r) for r in rows])
        df   = clean(df)
        load_dim_date(conn, df)
        load_dim_customer(conn, df)
        load_dim_product(conn, df)
        load_dim_region(conn, df)
        load_fact_sales(conn, df)
        logger.info("=== Transformation Complete ===")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
