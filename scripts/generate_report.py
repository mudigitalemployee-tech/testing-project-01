"""
HTML Report Generator (PRD Deliverable)
Queries the warehouse and produces a self-contained HTML report with:
- Pipeline run summary
- KPI cards: total sales, total profit, total orders, avg discount
- Top 10 products by sales
- Revenue by region
- Daily sales trend table
"""
import os
import sys
import base64
import io
from datetime import datetime

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import get_connection, load_config, execute_sql, get_logger

logger = get_logger(__name__)
PALETTE = ["#4E79A7", "#F28E2B", "#59A14F", "#E15759", "#76B7B2", "#EDC948"]


def _fig_to_b64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=110)
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return encoded


def _img(b64, width="100%"):
    return f'<img src="data:image/png;base64,{b64}" style="width:{width};max-width:960px;margin:10px 0;" />'


def fetch_kpis(conn):
    return execute_sql(conn, """
        SELECT
            ROUND(SUM(total_sales)::numeric, 2)  AS total_sales,
            ROUND(SUM(total_profit)::numeric, 2) AS total_profit,
            SUM(total_orders)                    AS total_orders,
            ROUND(AVG(avg_discount)::numeric, 4) AS avg_discount
        FROM agg_sales_summary;
    """, fetch=True)[0]


def fetch_top_products(conn):
    rows = execute_sql(conn, """
        SELECT p.product_name, ROUND(SUM(f.sales)::numeric, 2) AS total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_name
        ORDER BY total_sales DESC
        LIMIT 10;
    """, fetch=True)
    return pd.DataFrame([dict(r) for r in rows])


def fetch_region_sales(conn):
    rows = execute_sql(conn, """
        SELECT region, ROUND(SUM(total_sales)::numeric, 2) AS total_sales
        FROM agg_sales_summary
        GROUP BY region ORDER BY total_sales DESC;
    """, fetch=True)
    return pd.DataFrame([dict(r) for r in rows])


def fetch_daily_trend(conn):
    rows = execute_sql(conn, """
        SELECT agg_date, ROUND(SUM(total_sales)::numeric, 2) AS daily_sales
        FROM agg_sales_summary
        GROUP BY agg_date ORDER BY agg_date
        LIMIT 30;
    """, fetch=True)
    return pd.DataFrame([dict(r) for r in rows])


def fetch_category_summary(conn):
    rows = execute_sql(conn, """
        SELECT category,
               ROUND(SUM(total_sales)::numeric,2)  AS total_sales,
               ROUND(SUM(total_profit)::numeric,2) AS total_profit,
               SUM(total_orders)                   AS total_orders
        FROM agg_sales_summary
        GROUP BY category ORDER BY total_sales DESC;
    """, fetch=True)
    return pd.DataFrame([dict(r) for r in rows])


def plot_top_products(df):
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(df["product_name"][::-1], df["total_sales"][::-1],
            color=PALETTE[0], edgecolor="white")
    ax.set_title("Top 10 Products by Sales", fontweight="bold", fontsize=13)
    ax.set_xlabel("Total Sales")
    plt.tight_layout()
    return fig


def plot_region_sales(df):
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(df["region"], df["total_sales"],
           color=PALETTE[:len(df)], edgecolor="white")
    ax.set_title("Total Sales by Region", fontweight="bold", fontsize=13)
    ax.set_ylabel("Sales")
    for i, v in enumerate(df["total_sales"]):
        ax.text(i, v * 1.01, f"{v:,.0f}", ha="center", fontsize=9)
    plt.tight_layout()
    return fig


def plot_daily_trend(df):
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(pd.to_datetime(df["agg_date"]), df["daily_sales"],
            color=PALETTE[0], linewidth=1.8, marker="o", markersize=3)
    ax.set_title("Daily Sales Trend (Last 30 Days)", fontweight="bold", fontsize=13)
    ax.set_xlabel("Date")
    ax.set_ylabel("Sales")
    plt.xticks(rotation=30)
    plt.tight_layout()
    return fig


def generate_report(config_path=None, output_path=None):
    config = load_config(config_path)
    conn   = get_connection(config)
    now    = datetime.now().strftime("%Y-%m-%d %H:%M")

    if output_path is None:
        reports_dir = config["pipeline"].get("reports_dir", "reports")
        os.makedirs(reports_dir, exist_ok=True)
        output_path = os.path.join(reports_dir, "pipeline_report.html")

    try:
        kpis       = fetch_kpis(conn)
        products   = fetch_top_products(conn)
        regions    = fetch_region_sales(conn)
        daily      = fetch_daily_trend(conn)
        categories = fetch_category_summary(conn)
    finally:
        conn.close()

    # Charts
    prod_b64   = _fig_to_b64(plot_top_products(products))
    region_b64 = _fig_to_b64(plot_region_sales(regions))
    trend_b64  = _fig_to_b64(plot_daily_trend(daily))

    # Tables
    cat_table  = categories.to_html(index=False, border=0, classes="tbl")
    prod_table = products.to_html(index=False, border=0, classes="tbl")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>Sales Analytics Pipeline Report</title>
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css"/>
  <style>
    body{{font-family:'Segoe UI',Arial,sans-serif;background:#f7f8fa;color:#2c3e50;}}
    .navbar{{background:#2c3e50;border:none;border-radius:0;}}
    .navbar-brand{{color:#ecf0f1!important;}}
    .wrap{{max-width:1180px;margin:auto;padding:28px 18px;}}
    h1{{font-size:1.9rem;font-weight:700;}}
    h2{{font-size:1.3rem;font-weight:700;border-left:4px solid #4E79A7;
        padding-left:10px;margin-top:36px;color:#34495e;}}
    .card{{background:#fff;border-radius:8px;padding:20px 22px;
           margin-bottom:22px;box-shadow:0 2px 7px rgba(0,0,0,.07);}}
    .kpi{{display:flex;flex-wrap:wrap;gap:14px;}}
    .kpi-box{{background:#fff;border-radius:8px;padding:14px 18px;
              min-width:140px;flex:1;box-shadow:0 2px 6px rgba(0,0,0,.08);text-align:center;}}
    .kpi-box .val{{font-size:1.6rem;font-weight:700;color:#4E79A7;}}
    .kpi-box .lbl{{font-size:.8rem;color:#7f8c8d;margin-top:3px;}}
    .tbl{{width:100%;border-collapse:collapse;font-size:.88rem;}}
    .tbl th{{background:#2c3e50;color:#fff;padding:8px 11px;text-align:center;}}
    .tbl td{{padding:7px 11px;text-align:center;border-bottom:1px solid #ecf0f1;}}
    .tbl tr:nth-child(even){{background:#f4f6f7;}}
    .footer{{text-align:center;padding:18px;color:#95a5a6;font-size:.8rem;margin-top:36px;}}
    img{{border-radius:5px;}}
  </style>
</head>
<body>
<nav class="navbar navbar-default">
  <div class="container-fluid">
    <div class="navbar-header">
      <span class="navbar-brand">📊 Batch Sales Analytics Pipeline — Report</span>
    </div>
  </div>
</nav>
<div class="wrap">
  <div style="color:#95a5a6;font-size:.82rem;margin-bottom:6px;">Generated: {now}</div>
  <h1>Sales Analytics Pipeline Report</h1>
  <p style="color:#7f8c8d;">Superstore Sales Dataset — Batch ETL Pipeline (Apache Airflow + PostgreSQL)</p>

  <h2>1. KPI Summary</h2>
  <div class="card">
    <div class="kpi">
      <div class="kpi-box"><div class="val">${kpis['total_sales']:,.0f}</div><div class="lbl">Total Sales</div></div>
      <div class="kpi-box"><div class="val">${kpis['total_profit']:,.0f}</div><div class="lbl">Total Profit</div></div>
      <div class="kpi-box"><div class="val">{kpis['total_orders']:,}</div><div class="lbl">Total Orders</div></div>
      <div class="kpi-box"><div class="val">{float(kpis['avg_discount']):.1%}</div><div class="lbl">Avg Discount</div></div>
    </div>
  </div>

  <h2>2. Daily Sales Trend</h2>
  <div class="card">{_img(trend_b64)}</div>

  <h2>3. Sales by Region</h2>
  <div class="card">{_img(region_b64)}</div>

  <h2>4. Sales by Category</h2>
  <div class="card">{cat_table}</div>

  <h2>5. Top 10 Products by Sales</h2>
  <div class="card">
    {_img(prod_b64)}
    {prod_table}
  </div>

  <div class="footer">Batch Sales Analytics Pipeline &nbsp;|&nbsp; Generated {now}</div>
</div>
</body>
</html>"""

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"HTML report saved → {output_path}")
    return output_path


if __name__ == "__main__":
    generate_report()
