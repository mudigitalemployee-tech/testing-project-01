"""
Step 4 — Aggregation Layer (PRD 6.4)
Refreshes agg_sales_summary from fact_sales.
Grouped by: date × region × category
Metrics: total_sales, total_profit, total_orders, avg_discount
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import get_connection, load_config, execute_sql, get_logger

logger = get_logger(__name__)

AGG_SQL = """
INSERT INTO agg_sales_summary
    (agg_date, region, category, total_sales, total_profit, total_orders, avg_discount, refreshed_at)
SELECT
    f.order_date                        AS agg_date,
    f.region                            AS region,
    p.category                          AS category,
    ROUND(SUM(f.sales)::numeric, 2)     AS total_sales,
    ROUND(SUM(f.profit)::numeric, 2)    AS total_profit,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    ROUND(AVG(f.discount)::numeric, 4)  AS avg_discount,
    NOW()                               AS refreshed_at
FROM fact_sales f
LEFT JOIN dim_product p ON f.product_id = p.product_id
GROUP BY f.order_date, f.region, p.category
ON CONFLICT (agg_date, region, category) DO UPDATE
SET total_sales  = EXCLUDED.total_sales,
    total_profit = EXCLUDED.total_profit,
    total_orders = EXCLUDED.total_orders,
    avg_discount = EXCLUDED.avg_discount,
    refreshed_at = EXCLUDED.refreshed_at;
"""


def run(config_path=None):
    config = load_config(config_path)
    conn   = get_connection(config)
    try:
        logger.info("Building agg_sales_summary ...")
        execute_sql(conn, AGG_SQL)
        result = execute_sql(conn, "SELECT COUNT(*) AS cnt FROM agg_sales_summary;", fetch=True)
        logger.info(f"agg_sales_summary: {result[0]['cnt']} rows refreshed.")
        logger.info("=== Aggregation Complete ===")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
