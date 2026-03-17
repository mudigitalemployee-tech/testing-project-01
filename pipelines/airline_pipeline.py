"""
Airline Passenger Satisfaction Dataset Pipeline — Satisfaction Prediction
Bronze → Silver → Gold
"""

import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, label_encode, scale_features

logger = get_logger("airline_pipeline")

REQUIRED_COLUMNS = ["satisfaction", "Class", "Flight Distance", "Departure Delay in Minutes"]
BRONZE_PATH = "data/bronze/airline_satisfaction.csv"
SILVER_PATH = "data/silver/airline_clean.csv"
GOLD_PATH = "data/gold/airline_features.csv"


def ingest(path: str) -> pd.DataFrame:
    logger.info(f"[BRONZE] Ingesting from {path}")
    df = pd.read_csv(path)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[SILVER] Starting transformations")

    dq_report = run_all_checks(df, REQUIRED_COLUMNS)
    logger.info(f"DQ Status: {dq_report['overall_status']}")

    df = remove_duplicates(df)
    df = fill_nulls(df, strategy="median")

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Encode categorical columns
    cat_cols = df.select_dtypes(include="object").columns.tolist()
    df = label_encode(df, cat_cols)

    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[GOLD] Feature engineering for satisfaction prediction")

    # Total delay
    dep_col = "departure_delay_in_minutes"
    arr_col = "arrival_delay_in_minutes"
    if dep_col in df.columns and arr_col in df.columns:
        df["total_delay"] = df[dep_col] + df[arr_col]
        df["is_delayed"] = (df["total_delay"] > 15).astype(int)
        logger.info("Created total_delay and is_delayed features")

    # Service score average (rating columns 1-5)
    service_cols = [c for c in df.columns if any(x in c for x in
                    ["inflight", "food", "seat", "cleanliness", "boarding",
                     "baggage", "checkin", "service", "wifi", "entertainment"])]
    if service_cols:
        df["avg_service_score"] = df[service_cols].mean(axis=1)
        logger.info(f"Created avg_service_score from {len(service_cols)} service columns")

    # Scale numeric features
    exclude = ["satisfaction", "id"]
    num_cols = [c for c in df.select_dtypes(include=["float64", "int64"]).columns
                if c not in exclude]
    if num_cols:
        df = scale_features(df, num_cols)

    # Satisfaction breakdown by class
    if "class" in df.columns and "satisfaction" in df.columns:
        class_sat = df.groupby("class")["satisfaction"].mean().reset_index()
        class_sat.columns = ["class", "avg_satisfaction"]
        os.makedirs("data/gold", exist_ok=True)
        class_sat.to_csv("data/gold/airline_class_satisfaction.csv", index=False)
        logger.info("Saved class satisfaction to data/gold/airline_class_satisfaction.csv")

    logger.info(f"[GOLD] Final shape: {df.shape}")
    return df


def save(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("AIRLINE PIPELINE STARTED")
    logger.info("=" * 50)
    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)
    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)
    logger.info("AIRLINE PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
