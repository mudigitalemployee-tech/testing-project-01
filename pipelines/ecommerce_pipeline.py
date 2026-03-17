"""
E-Commerce Sales Dataset Pipeline — Customer Behaviour & Revenue Analytics
Bronze → Silver → Gold
"""

import pandas as pd
import numpy as np
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, label_encode

logger = get_logger("ecommerce_pipeline")

REQUIRED_COLUMNS = ["InvoiceNo", "StockCode", "Quantity", "UnitPrice", "CustomerID", "Country"]
BRONZE_PATH = "data/bronze/ecommerce.csv"
SILVER_PATH = "data/silver/ecommerce_clean.csv"
GOLD_PATH = "data/gold/ecommerce_rfm.csv"


def ingest(path: str) -> pd.DataFrame:
    logger.info(f"[BRONZE] Ingesting from {path}")
    df = pd.read_csv(path, encoding="latin-1")
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[SILVER] Starting transformations")

    dq_report = run_all_checks(df, REQUIRED_COLUMNS)
    logger.info(f"DQ Status: {dq_report['overall_status']}")

    df = remove_duplicates(df)
    df = fill_nulls(df, strategy="median")

    # Parse InvoiceDate
    if "InvoiceDate" in df.columns:
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
        df["Year"] = df["InvoiceDate"].dt.year
        df["Month"] = df["InvoiceDate"].dt.month
        df["DayOfWeek"] = df["InvoiceDate"].dt.dayofweek

    # Remove cancellations (InvoiceNo starts with C)
    if "InvoiceNo" in df.columns:
        before = len(df)
        df = df[~df["InvoiceNo"].astype(str).str.startswith("C")]
        logger.info(f"Removed {before - len(df)} cancellation rows")

    # Remove negative quantities/prices
    if "Quantity" in df.columns:
        df = df[df["Quantity"] > 0]
    if "UnitPrice" in df.columns:
        df = df[df["UnitPrice"] > 0]

    # Revenue column
    df["Revenue"] = df["Quantity"] * df["UnitPrice"]

    # Encode country
    df = label_encode(df, ["Country"])

    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[GOLD] RFM Analysis")

    if "CustomerID" not in df.columns or "InvoiceDate" not in df.columns:
        logger.warning("Missing CustomerID or InvoiceDate — skipping RFM")
        return df

    snapshot_date = df["InvoiceDate"].max()

    rfm = df.groupby("CustomerID").agg(
        Recency=("InvoiceDate", lambda x: (snapshot_date - x.max()).days),
        Frequency=("InvoiceNo", "nunique"),
        Monetary=("Revenue", "sum")
    ).reset_index()

    # RFM scoring (1-4 quartile ranks)
    rfm["R_Score"] = pd.qcut(rfm["Recency"], q=4, labels=[4, 3, 2, 1]).astype(int)
    rfm["F_Score"] = pd.qcut(rfm["Frequency"].rank(method="first"), q=4, labels=[1, 2, 3, 4]).astype(int)
    rfm["M_Score"] = pd.qcut(rfm["Monetary"].rank(method="first"), q=4, labels=[1, 2, 3, 4]).astype(int)
    rfm["RFM_Score"] = rfm["R_Score"] + rfm["F_Score"] + rfm["M_Score"]

    # Customer segment
    rfm["Segment"] = pd.cut(rfm["RFM_Score"], bins=[2, 5, 8, 12],
                              labels=["At_Risk", "Potential", "Champion"])

    logger.info(f"RFM Segments:\n{rfm['Segment'].value_counts().to_string()}")

    # Monthly revenue trend
    if "Month" in df.columns and "Year" in df.columns:
        monthly_rev = df.groupby(["Year", "Month"])["Revenue"].sum().reset_index()
        os.makedirs("data/gold", exist_ok=True)
        monthly_rev.to_csv("data/gold/ecommerce_monthly_revenue.csv", index=False)
        logger.info("Saved monthly revenue to data/gold/ecommerce_monthly_revenue.csv")

    logger.info(f"[GOLD] RFM shape: {rfm.shape}")
    return rfm


def save(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("E-COMMERCE PIPELINE STARTED")
    logger.info("=" * 50)
    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)
    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)
    logger.info("E-COMMERCE PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
