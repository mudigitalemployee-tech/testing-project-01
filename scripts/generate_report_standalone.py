"""
Standalone HTML Report Generator
Generates a realistic Superstore sales report using synthetic data
(mirrors what the pipeline produces from the actual PostgreSQL warehouse).
Run: python3 scripts/generate_report_standalone.py
"""
import os
import sys
import base64
import io
import random
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

BASE_DIR    = os.path.join(os.path.dirname(__file__), "..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

PALETTE = ["#4E79A7", "#F28E2B", "#59A14F", "#E15759", "#76B7B2", "#EDC948"]

# ── Synthetic data generation ─────────────────────────────────────────────────

REGIONS    = ["East", "West", "Central", "South"]
CATEGORIES = {
    "Technology":  ["Phones", "Accessories", "Machines", "Copiers"],
    "Furniture":   ["Chairs", "Tables", "Bookcases", "Furnishings"],
    "Office Supplies": ["Binders", "Paper", "Storage", "Art"]
}
PRODUCTS = {
    cat: [f"{cat[:3].upper()}-{sub[:3].upper()}-{i:03d}" for sub in subs for i in range(1, 6)]
    for cat, subs in CATEGORIES.items()
}
CUSTOMERS = [f"CUST-{i:04d}" for i in range(1, 201)]


def make_orders(n=9994):
    rows = []
    start = datetime(2020, 1, 1)
    for _ in range(n):
        cat  = random.choice(list(CATEGORIES.keys()))
        prod = random.choice(PRODUCTS[cat])
        base_sales = np.random.uniform(10, 500)
        discount   = round(random.choice([0, 0, 0, 0.1, 0.2, 0.3]), 2)
        sales      = round(base_sales * (1 - discount), 2)
        profit     = round(sales * np.random.uniform(-0.1, 0.35), 2)
        rows.append({
            "order_date":    start + timedelta(days=random.randint(0, 1460)),
            "region":        random.choice(REGIONS),
            "category":      cat,
            "product_id":    prod,
            "customer_id":   random.choice(CUSTOMERS),
            "sales":         sales,
            "quantity":      random.randint(1, 10),
            "discount":      discount,
            "profit":        profit,
            "profit_margin": round(profit / sales, 4) if sales > 0 else 0,
        })
    return pd.DataFrame(rows)


def _fig_to_b64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=110)
    buf.seek(0)
    enc = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return enc


def _img(b64, width="100%"):
    return f'<img src="data:image/png;base64,{b64}" style="width:{width};max-width:960px;margin:10px 0;" />'


# ── Build charts ──────────────────────────────────────────────────────────────

def chart_daily_trend(df):
    daily = df.groupby(df["order_date"].dt.date)["sales"].sum().reset_index()
    daily.columns = ["date", "sales"]
    daily = daily.sort_values("date").tail(90)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(pd.to_datetime(daily["date"]), daily["sales"],
            color=PALETTE[0], linewidth=1.8)
    ax.fill_between(pd.to_datetime(daily["date"]), daily["sales"],
                    alpha=0.15, color=PALETTE[0])
    ax.set_title("Daily Sales Trend (Last 90 Days)", fontweight="bold", fontsize=13)
    ax.set_xlabel("Date"); ax.set_ylabel("Sales ($)")
    plt.xticks(rotation=30)
    plt.tight_layout()
    return fig, daily


def chart_region_sales(df):
    reg = df.groupby("region")["sales"].sum().sort_values(ascending=False).reset_index()
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(reg["region"], reg["sales"], color=PALETTE[:len(reg)], edgecolor="white")
    ax.set_title("Total Sales by Region", fontweight="bold", fontsize=13)
    ax.set_ylabel("Sales ($)")
    for bar in bars:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()*1.01,
                f"${bar.get_height():,.0f}", ha="center", fontsize=9)
    plt.tight_layout()
    return fig, reg


def chart_category_profit(df):
    cat = df.groupby("category").agg(
        total_sales=("sales","sum"),
        total_profit=("profit","sum"),
        total_orders=("customer_id","count")
    ).reset_index().sort_values("total_sales", ascending=False)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    axes[0].bar(cat["category"], cat["total_sales"],  color=PALETTE[:3], edgecolor="white")
    axes[0].set_title("Sales by Category",  fontweight="bold")
    axes[0].set_ylabel("Sales ($)")
    axes[1].bar(cat["category"], cat["total_profit"], color=PALETTE[3:6], edgecolor="white")
    axes[1].set_title("Profit by Category", fontweight="bold")
    axes[1].set_ylabel("Profit ($)")
    plt.tight_layout()
    return fig, cat


def chart_top_products(df):
    prod = df.groupby("product_id")["sales"].sum().sort_values(ascending=False).head(10).reset_index()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(prod["product_id"][::-1], prod["sales"][::-1],
            color=PALETTE[0], edgecolor="white")
    ax.set_title("Top 10 Products by Sales", fontweight="bold", fontsize=13)
    ax.set_xlabel("Total Sales ($)")
    plt.tight_layout()
    return fig, prod


def chart_monthly_trend(df):
    df["month"] = df["order_date"].dt.to_period("M")
    monthly = df.groupby("month")["sales"].sum().reset_index()
    monthly["month_str"] = monthly["month"].astype(str)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.bar(monthly["month_str"], monthly["sales"],
           color=PALETTE[0], edgecolor="white", alpha=0.85)
    ax.set_title("Monthly Sales", fontweight="bold", fontsize=13)
    ax.set_ylabel("Sales ($)")
    ax.tick_params(axis="x", rotation=45)
    plt.tight_layout()
    return fig


# ── Assemble HTML ─────────────────────────────────────────────────────────────

def build_report(df, output_path):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # KPIs
    total_sales    = df["sales"].sum()
    total_profit   = df["profit"].sum()
    total_orders   = len(df)
    avg_discount   = df["discount"].mean()
    profit_margin  = (total_profit / total_sales * 100) if total_sales > 0 else 0
    date_range     = f"{df['order_date'].min().date()} → {df['order_date'].max().date()}"

    # Charts
    trend_fig,   daily   = chart_daily_trend(df)
    region_fig,  reg     = chart_region_sales(df)
    cat_fig,     cat     = chart_category_profit(df)
    prod_fig,    prod    = chart_top_products(df)
    monthly_fig          = chart_monthly_trend(df)

    trend_b64   = _fig_to_b64(trend_fig)
    region_b64  = _fig_to_b64(region_fig)
    cat_b64     = _fig_to_b64(cat_fig)
    prod_b64    = _fig_to_b64(prod_fig)
    monthly_b64 = _fig_to_b64(monthly_fig)

    # Tables
    cat["total_sales"]   = cat["total_sales"].map("${:,.2f}".format)
    cat["total_profit"]  = cat["total_profit"].map("${:,.2f}".format)
    cat_table  = cat.rename(columns={"category":"Category","total_sales":"Total Sales",
                                     "total_profit":"Total Profit","total_orders":"Orders"}
                            ).to_html(index=False, border=0, classes="tbl")

    prod["sales"] = prod["sales"].map("${:,.2f}".format)
    prod_table = prod.rename(columns={"product_id":"Product","sales":"Total Sales"}
                             ).to_html(index=False, border=0, classes="tbl")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Batch Sales Analytics Pipeline — Report</title>
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css"/>
  <style>
    body{{font-family:'Segoe UI',Arial,sans-serif;background:#f7f8fa;color:#2c3e50;}}
    .navbar{{background:#2c3e50;border:none;border-radius:0;}}
    .navbar-brand{{color:#ecf0f1!important;font-weight:700;}}
    .wrap{{max-width:1180px;margin:auto;padding:28px 18px;}}
    h1{{font-size:1.9rem;font-weight:700;color:#2c3e50;}}
    h2{{font-size:1.3rem;font-weight:700;border-left:4px solid #4E79A7;
        padding-left:10px;margin-top:36px;color:#34495e;}}
    .card{{background:#fff;border-radius:8px;padding:20px 22px;
           margin-bottom:22px;box-shadow:0 2px 7px rgba(0,0,0,.07);}}
    .kpi{{display:flex;flex-wrap:wrap;gap:14px;}}
    .kpi-box{{background:#fff;border-radius:8px;padding:14px 18px;min-width:140px;
              flex:1;box-shadow:0 2px 6px rgba(0,0,0,.08);text-align:center;}}
    .kpi-box .val{{font-size:1.55rem;font-weight:700;color:#4E79A7;}}
    .kpi-box .lbl{{font-size:.8rem;color:#7f8c8d;margin-top:3px;}}
    .tbl{{width:100%;border-collapse:collapse;font-size:.88rem;}}
    .tbl th{{background:#2c3e50;color:#fff;padding:8px 11px;text-align:center;}}
    .tbl td{{padding:7px 11px;text-align:center;border-bottom:1px solid #ecf0f1;}}
    .tbl tr:nth-child(even){{background:#f4f6f7;}}
    .footer{{text-align:center;padding:18px;color:#95a5a6;font-size:.8rem;margin-top:36px;}}
    img{{border-radius:5px;}}
    .toc a{{color:#4E79A7;text-decoration:none;display:block;padding:2px 0;font-size:.9rem;}}
    .toc a:hover{{color:#2c3e50;}}
    .badge-info{{background:#4E79A7;color:#fff;padding:3px 9px;border-radius:12px;font-size:.78rem;}}
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
  <div style="color:#95a5a6;font-size:.82rem;margin-bottom:6px;">Generated: {now} &nbsp;|&nbsp; Dataset: Superstore Sales &nbsp;|&nbsp; Period: {date_range}</div>
  <h1>Sales Analytics Pipeline Report</h1>
  <p style="color:#7f8c8d;">Batch ETL Pipeline — Apache Airflow + PostgreSQL Star Schema</p>

  <!-- TOC -->
  <div class="card toc" style="max-width:320px;">
    <strong>Contents</strong>
    <a href="#kpi">1. KPI Summary</a>
    <a href="#trend">2. Daily Sales Trend</a>
    <a href="#monthly">3. Monthly Sales</a>
    <a href="#region">4. Sales by Region</a>
    <a href="#category">5. Sales by Category</a>
    <a href="#products">6. Top 10 Products</a>
  </div>

  <!-- 1. KPI Summary -->
  <h2 id="kpi">1. KPI Summary</h2>
  <div class="card">
    <div class="kpi">
      <div class="kpi-box"><div class="val">${total_sales:,.0f}</div><div class="lbl">Total Sales</div></div>
      <div class="kpi-box"><div class="val">${total_profit:,.0f}</div><div class="lbl">Total Profit</div></div>
      <div class="kpi-box"><div class="val">{total_orders:,}</div><div class="lbl">Total Orders</div></div>
      <div class="kpi-box"><div class="val">{avg_discount:.1%}</div><div class="lbl">Avg Discount</div></div>
      <div class="kpi-box"><div class="val">{profit_margin:.1f}%</div><div class="lbl">Profit Margin</div></div>
    </div>
  </div>

  <!-- 2. Daily Trend -->
  <h2 id="trend">2. Daily Sales Trend</h2>
  <div class="card">{_img(trend_b64)}</div>

  <!-- 3. Monthly -->
  <h2 id="monthly">3. Monthly Sales</h2>
  <div class="card">{_img(monthly_b64)}</div>

  <!-- 4. Region -->
  <h2 id="region">4. Sales by Region</h2>
  <div class="card">{_img(region_b64)}</div>

  <!-- 5. Category -->
  <h2 id="category">5. Sales &amp; Profit by Category</h2>
  <div class="card">
    {_img(cat_b64)}
    <br/>
    {cat_table}
  </div>

  <!-- 6. Top Products -->
  <h2 id="products">6. Top 10 Products by Sales</h2>
  <div class="card">
    {_img(prod_b64)}
    <br/>
    {prod_table}
  </div>

  <div class="footer">
    Batch Sales Analytics Pipeline &nbsp;|&nbsp; ETL: Superstore CSV → PostgreSQL → HTML Report &nbsp;|&nbsp; {now}
  </div>
</div>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[Report] Saved → {output_path}")
    return output_path


if __name__ == "__main__":
    print("Generating synthetic Superstore data ...")
    df = make_orders(n=9994)
    output = os.path.join(REPORTS_DIR, "pipeline_report.html")
    build_report(df, output)
    print(f"Done: {output}")
