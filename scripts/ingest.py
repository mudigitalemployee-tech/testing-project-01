"""
ingest.py — Phase 1: Ingestion Layer
Loads superstore.csv into stg_superstore (PostgreSQL).
Validates: file exists, schema match, no corrupt rows.
"""

import os
import sys
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [INGEST] %(message)s")
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────
DB_CONN = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "salesdb"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

REQUIRED_COLUMNS = {
    "order_id", "order_date", "ship_date", "customer_id",
    "region", "state", "category", "sub_category", "product_id",
    "sales", "quantity", "discount", "profit"
}

CSV_PATH = os.getenv("CSV_PATH", os.path.join(os.path.dirname(__file__), "../data/superstore.csv"))


def validate_file(path: str) -> None:
    """Check file exists."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")
    log.info(f"File found: {path}")


def validate_schema(df: pd.DataFrame) -> None:
    """Check all required columns are present."""
    missing = REQUIRED_COLUMNS - set(df.columns.str.lower())
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    log.info(f"Schema validated — {len(df.columns)} columns present")


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Drop corrupt rows (unparseable dates, non-numeric sales)."""
    original = len(df)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["ship_date"]  = pd.to_datetime(df["ship_date"],  errors="coerce")
    df["sales"]      = pd.to_numeric(df["sales"],    errors="coerce")
    df["profit"]     = pd.to_numeric(df["profit"],   errors="coerce")
    df["quantity"]   = pd.to_numeric(df["quantity"], errors="coerce")
    df["discount"]   = pd.to_numeric(df["discount"], errors="coerce")
    df = df.dropna(subset=["order_date", "ship_date", "sales", "profit", "quantity"])
    dropped = original - len(df)
    log.info(f"Corrupt rows dropped: {dropped} | Clean rows: {len(df)}")
    return df


def load_to_staging(df: pd.DataFrame) -> int:
    """Truncate stg_superstore and bulk-insert all rows."""
    conn = psycopg2.connect(**DB_CONN)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE stg_superstore RESTART IDENTITY;")
                records = [
                    (
                        row["order_id"],
                        row["order_date"].date() if hasattr(row["order_date"], "date") else row["order_date"],
                        row["ship_date"].date()  if hasattr(row["ship_date"],  "date") else row["ship_date"],
                        row["customer_id"],
                        row["region"],
                        row["state"],
                        row["category"],
                        row["sub_category"],
                        row["product_id"],
                        float(row["sales"]),
                        int(row["quantity"]),
                        float(row["discount"]),
                        float(row["profit"]),
                    )
                    for _, row in df.iterrows()
                ]
                execute_values(
                    cur,
                    """INSERT INTO stg_superstore
                       (order_id, order_date, ship_date, customer_id,
                        region, state, category, sub_category, product_id,
                        sales, quantity, discount, profit)
                       VALUES %s""",
                    records,
                    page_size=1000
                )
                log.info(f"Inserted {len(records)} rows into stg_superstore")
                return len(records)
    finally:
        conn.close()


def run():
    log.info("=== INGEST PHASE START ===")
    validate_file(CSV_PATH)
    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    validate_schema(df)
    df = validate_data(df)
    count = load_to_staging(df)
    log.info(f"=== INGEST PHASE COMPLETE — {count} rows staged ===")
    return count


if __name__ == "__main__":
    run()
