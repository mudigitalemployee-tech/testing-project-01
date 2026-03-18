"""
validate.py — Phase 2: Data Quality Validation
Runs DQ checks on stg_superstore before transformation.
Rules: row count, null %, duplicates, sales >= 0.
"""

import os
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [VALIDATE] %(message)s")
log = logging.getLogger(__name__)

DB_CONN = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "salesdb"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

NULL_THRESHOLD = 0.05   # 5% max nulls
MIN_ROWS       = 100    # Minimum expected rows


def run_checks() -> dict:
    conn = psycopg2.connect(**DB_CONN)
    results = {}

    with conn:
        with conn.cursor() as cur:

            # 1. Row count
            cur.execute("SELECT COUNT(*) FROM stg_superstore;")
            total = cur.fetchone()[0]
            results["total_rows"] = total
            results["row_count_ok"] = total >= MIN_ROWS
            log.info(f"[CHECK] Row count: {total} — {'PASS' if results['row_count_ok'] else 'FAIL'}")

            # 2. Null % on critical columns
            for col in ["sales", "profit", "order_date", "customer_id"]:
                cur.execute(f"SELECT COUNT(*) FROM stg_superstore WHERE {col} IS NULL;")
                null_count = cur.fetchone()[0]
                null_pct = null_count / total if total > 0 else 0
                key = f"null_pct_{col}"
                results[key] = null_pct
                status = "PASS" if null_pct <= NULL_THRESHOLD else "FAIL"
                log.info(f"[CHECK] Null % in {col}: {null_pct:.2%} — {status}")

            # 3. Duplicates (order_id + product_id)
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT order_id, product_id, COUNT(*) AS cnt
                    FROM stg_superstore
                    GROUP BY order_id, product_id
                    HAVING COUNT(*) > 1
                ) dup;
            """)
            dup_count = cur.fetchone()[0]
            results["duplicate_groups"] = dup_count
            results["duplicates_ok"] = dup_count == 0
            log.info(f"[CHECK] Duplicate groups: {dup_count} — {'PASS' if dup_count == 0 else 'WARN'}")

            # 4. Negative sales
            cur.execute("SELECT COUNT(*) FROM stg_superstore WHERE sales < 0;")
            neg_sales = cur.fetchone()[0]
            results["negative_sales"] = neg_sales
            results["sales_ok"] = neg_sales == 0
            log.info(f"[CHECK] Negative sales rows: {neg_sales} — {'PASS' if neg_sales == 0 else 'FAIL'}")

    conn.close()

    # Overall pass/fail
    critical_checks = [
        results["row_count_ok"],
        results["sales_ok"],
        all(results[f"null_pct_{c}"] <= NULL_THRESHOLD for c in ["sales", "profit", "order_date", "customer_id"])
    ]
    results["overall_pass"] = all(critical_checks)

    if not results["overall_pass"]:
        raise ValueError(f"Data quality checks FAILED: {results}")

    log.info(f"=== ALL CHECKS PASSED ===")
    return results


if __name__ == "__main__":
    run_checks()
