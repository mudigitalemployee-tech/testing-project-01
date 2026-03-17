"""
Run all Kaggle pipelines sequentially.
Usage: python run_all.py
"""

import sys
import os
import traceback
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logger import get_logger
from pipelines.titanic_pipeline import run as run_titanic
from pipelines.house_prices_pipeline import run as run_house_prices
from pipelines.netflix_pipeline import run as run_netflix
from pipelines.fraud_pipeline import run as run_fraud
from pipelines.retail_sales_pipeline import run as run_retail_sales
from pipelines.iris_pipeline import run as run_iris
from pipelines.diabetes_pipeline import run as run_diabetes
from pipelines.spotify_pipeline import run as run_spotify
from pipelines.ecommerce_pipeline import run as run_ecommerce
from pipelines.airline_pipeline import run as run_airline

logger = get_logger("run_all")

PIPELINES = [
    ("Titanic", run_titanic),
    ("House Prices", run_house_prices),
    ("Netflix", run_netflix),
    ("Credit Card Fraud", run_fraud),
    ("Retail Sales", run_retail_sales),
    ("Iris", run_iris),
    ("Diabetes", run_diabetes),
    ("Spotify Tracks", run_spotify),
    ("E-Commerce", run_ecommerce),
    ("Airline Satisfaction", run_airline),
]


def main():
    logger.info("=" * 60)
    logger.info(f"ALL PIPELINES STARTED — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    results = {}
    for name, pipeline_fn in PIPELINES:
        try:
            logger.info(f"\n>>> Running: {name} Pipeline")
            pipeline_fn()
            results[name] = "✓ SUCCESS"
        except FileNotFoundError as e:
            results[name] = f"⚠ SKIPPED (source file not found: {e})"
            logger.warning(f"{name} skipped — {e}")
        except Exception as e:
            results[name] = f"✗ FAILED — {str(e)}"
            logger.error(f"{name} failed: {traceback.format_exc()}")

    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    for name, status in results.items():
        logger.info(f"  {name:<25} {status}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
