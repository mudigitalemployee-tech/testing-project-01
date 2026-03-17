"""
Iris Dataset Pipeline — Species Classification & Feature Engineering
Bronze → Silver → Gold
"""

import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, label_encode, scale_features

logger = get_logger("iris_pipeline")

REQUIRED_COLUMNS = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
BRONZE_PATH = "data/bronze/iris.csv"
SILVER_PATH = "data/silver/iris_clean.csv"
GOLD_PATH = "data/gold/iris_features.csv"


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

    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[GOLD] Feature engineering for classification")

    # Petal area and sepal area
    df["petal_area"] = df["petal_length"] * df["petal_width"]
    df["sepal_area"] = df["sepal_length"] * df["sepal_width"]

    # Petal/sepal ratio
    df["petal_sepal_ratio"] = df["petal_area"] / df["sepal_area"]

    # Scale numeric features
    num_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width", "petal_area", "sepal_area"]
    df = scale_features(df, num_cols)

    # Encode target
    df = label_encode(df, ["species"])

    logger.info(f"[GOLD] Final shape: {df.shape}")
    return df


def save(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("IRIS PIPELINE STARTED")
    logger.info("=" * 50)
    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)
    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)
    logger.info("IRIS PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
