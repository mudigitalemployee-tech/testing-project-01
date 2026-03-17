"""
Spotify Tracks Dataset Pipeline — Music Analytics & Popularity Prediction
Bronze → Silver → Gold
"""

import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from utils.data_quality import run_all_checks
from utils.transformations import fill_nulls, remove_duplicates, label_encode, scale_features

logger = get_logger("spotify_pipeline")

REQUIRED_COLUMNS = ["track_name", "artist_name", "genre", "popularity", "danceability", "energy"]
BRONZE_PATH = "data/bronze/spotify_tracks.csv"
SILVER_PATH = "data/silver/spotify_clean.csv"
GOLD_PATH = "data/gold/spotify_features.csv"


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

    # Encode genre
    if "genre" in df.columns:
        df = label_encode(df, ["genre"])

    logger.info(f"[SILVER] Transformed shape: {df.shape}")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[GOLD] Feature engineering for popularity prediction")

    # Energy-danceability score
    if "energy" in df.columns and "danceability" in df.columns:
        df["energy_dance_score"] = (df["energy"] + df["danceability"]) / 2

    # Popularity tier
    if "popularity" in df.columns:
        df["popularity_tier"] = pd.cut(df["popularity"], bins=[0, 25, 50, 75, 100],
                                        labels=["Low", "Medium", "High", "Viral"])
        df = label_encode(df, ["popularity_tier"])

    # Acoustic vs Electronic index
    if "acousticness" in df.columns and "energy" in df.columns:
        df["acoustic_electronic_index"] = df["acousticness"] - df["energy"]

    # Scale audio features
    audio_features = [c for c in ["danceability", "energy", "valence", "tempo",
                                    "acousticness", "speechiness", "liveness",
                                    "energy_dance_score"] if c in df.columns]
    if audio_features:
        df = scale_features(df, audio_features)

    # Genre-level aggregation
    if "genre" in df.columns and "popularity" in df.columns:
        genre_agg = df.groupby("genre")["popularity"].agg(["mean", "count"]).reset_index()
        genre_agg.columns = ["genre", "avg_popularity", "track_count"]
        os.makedirs("data/gold", exist_ok=True)
        genre_agg.to_csv("data/gold/spotify_genre_stats.csv", index=False)
        logger.info("Saved genre stats to data/gold/spotify_genre_stats.csv")

    logger.info(f"[GOLD] Final shape: {df.shape}")
    return df


def save(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df)} rows to {path}")


def run():
    logger.info("=" * 50)
    logger.info("SPOTIFY PIPELINE STARTED")
    logger.info("=" * 50)
    df_bronze = ingest(BRONZE_PATH)
    df_silver = transform(df_bronze)
    save(df_silver, SILVER_PATH)
    df_gold = aggregate(df_silver)
    save(df_gold, GOLD_PATH)
    logger.info("SPOTIFY PIPELINE COMPLETED ✓")


if __name__ == "__main__":
    run()
