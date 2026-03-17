"""
Diabetes Dataset Pipeline — Disease Prediction & Risk Scoring
Bronze → Silver → Gold
"""

import pandas as pd
import numpy as np
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, scale_features

logger = get_logger("diabetes_pipeline")

REQUIRED_COLUMNS = ["Glucose", "BMI", "Age", "Outcome"]
BRONZE_PATH = "data/bronze/diabetes.csv"
SILVER_PATH = "data/silver/diabetes_clean.csv"
GOLD_PATH = "data/gold/diabetes_features.csv"


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

    # Replace biologically impossible 0s with NaN for specific columns
    zero_invalid_cols = ["Glucose", "BloodPressure", "SkinThickness", "Insulin", "BMI"]
    for col in zero_invalid_cols:
        if col in df.columns:
            count = (df[col] == 0).sum()
            df[col] = df[col].replace(0, np.nan)
            logger.info(f"Replaced {count} zero values in '{col}' with NaN")

    df = fill_nulls(df, strategy="median")
    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[GOLD] Feature engineering for risk scoring")

    # BMI category
    if "BMI" in df.columns:
        df["BMI_Category"] = pd.cut(df["BMI"], bins=[0, 18.5, 24.9, 29.9, 100],
                                     labels=["Underweight", "Normal", "Overweight", "Obese"])
        from utils.transformations import label_encode
        df = label_encode(df, ["BMI_Category"])

    # Glucose risk tier
    if "Glucose" in df.columns:
        df["Glucose_Risk"] = pd.cut(df["Glucose"], bins=[0, 99, 125, 300],
                                     labels=["Normal", "Prediabetic", "Diabetic"])
        from utils.transformations import label_encode
        df = label_encode(df, ["Glucose_Risk"])

    # Age group
    if "Age" in df.columns:
        df["AgeGroup"] = pd.cut(df["Age"], bins=[0, 30, 45, 60, 100],
                                 labels=["Young", "MiddleAge", "Senior", "Elderly"])
        from utils.transformations import label_encode
        df = label_encode(df, ["AgeGroup"])

    # Scale features
    num_cols = [c for c in df.columns if c not in ["Outcome"]]
    df = scale_features(df, num_cols)

    logger.info(f"[GOLD] Final shape: {df.shape}")
    return df


def save(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("DIABETES PIPELINE STARTED")
    logger.info("=" * 50)
    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)
    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)
    logger.info("DIABETES PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
