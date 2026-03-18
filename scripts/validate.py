"""
Step 2 — Data Validation Layer (PRD 8)
Quality checks on stg_superstore:
  - Row count > 0
  - Null % on critical columns < 5%
  - Duplicate (order_id, product_id) — warning
  - sales >= 0 constraint
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import get_connection, load_config, execute_sql, get_logger

logger = get_logger(__name__)
MAX_NULL_PCT = 0.05


def check_row_count(conn) -> int:
    rows = execute_sql(conn, "SELECT COUNT(*) AS cnt FROM stg_superstore;", fetch=True)
    count = rows[0]["cnt"]
    if count == 0:
        raise ValueError("Quality FAIL: stg_superstore is empty.")
    logger.info(f"Row count PASS: {count} rows")
    return count


def check_nulls(conn):
    critical = ["order_id", "order_date", "customer_id", "product_id", "sales", "profit"]
    total = execute_sql(conn, "SELECT COUNT(*) AS cnt FROM stg_superstore;", fetch=True)[0]["cnt"]
    for col in critical:
        res = execute_sql(conn, f"SELECT COUNT(*) AS cnt FROM stg_superstore WHERE {col} IS NULL;", fetch=True)
        pct = res[0]["cnt"] / total if total > 0 else 0
        if pct > MAX_NULL_PCT:
            raise ValueError(f"Quality FAIL: '{col}' has {pct:.1%} nulls (max {MAX_NULL_PCT:.0%})")
        logger.info(f"Null check PASS [{col}]: {pct:.2%}")


def check_duplicates(conn):
    res = execute_sql(conn, """
        SELECT COUNT(*) AS cnt FROM (
            SELECT order_id, product_id FROM stg_superstore
            GROUP BY order_id, product_id HAVING COUNT(*) > 1
        ) d;""", fetch=True)
    n = res[0]["cnt"]
    if n > 0:
        logger.warning(f"Duplicate WARNING: {n} pairs — will deduplicate in transform.")
    else:
        logger.info("Duplicate check PASS.")
    return n


def check_sales_constraint(conn):
    res = execute_sql(conn, "SELECT COUNT(*) AS cnt FROM stg_superstore WHERE sales < 0;", fetch=True)
    if res[0]["cnt"] > 0:
        raise ValueError(f"Quality FAIL: {res[0]['cnt']} rows with sales < 0.")
    logger.info("Sales >= 0 PASS.")


def run(config_path=None):
    config = load_config(config_path)
    conn   = get_connection(config)
    try:
        logger.info("=== Data Validation START ===")
        check_row_count(conn)
        check_nulls(conn)
        check_duplicates(conn)
        check_sales_constraint(conn)
        logger.info("=== Data Validation PASSED ===")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
