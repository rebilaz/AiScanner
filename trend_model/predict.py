"""Generate daily price trend predictions using a trained model."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import List

import joblib
import pandas as pd
from google.cloud import bigquery

from .train import create_features

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SOURCE_TABLE = os.getenv("BQ_PRICE_FEATURES_TABLE", "asset_price_features")
DEST_TABLE = os.getenv("BQ_PREDICTIONS_TABLE", "asset_trend_predictions")
MODEL_PATH = os.getenv("PRICE_MODEL_PATH", os.path.join(os.path.dirname(__file__), "model.pkl"))
PREDICTION_WINDOW = int(os.getenv("PREDICTION_WINDOW_DAYS", "30"))

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_recent_data(client: bigquery.Client) -> pd.DataFrame:
    """Load recent data to generate predictions."""
    sql = f"""
        SELECT * FROM `{client.project}.{DATASET}.{SOURCE_TABLE}`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {PREDICTION_WINDOW} DAY)
    """
    return client.query(sql).to_dataframe()

# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------

def load_model() -> tuple[object, List[str]]:
    """Load the trained model and feature names."""
    obj = joblib.load(MODEL_PATH)
    return obj["model"], obj["features"]


def generate_predictions(model: object, features: List[str], df: pd.DataFrame) -> pd.DataFrame:
    """Return prediction probabilities for the given dataframe."""
    if df.empty:
        return df
    probs = model.predict_proba(df[features])[:, 1]
    df = df[["asset_id", "timestamp"]].copy()
    df["predicted_prob"] = probs
    df["generation_time"] = datetime.utcnow()
    return df


def store_predictions(client: bigquery.Client, df: pd.DataFrame) -> None:
    """Append predictions to BigQuery table."""
    if df.empty:
        logging.info("No predictions to store")
        return
    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Uploading %d predictions to %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    model, feature_cols = load_model()
    client = bigquery.Client(project=PROJECT_ID)

    df = load_recent_data(client)
    if df.empty:
        logging.error("No recent data for prediction")
        return

    df = create_features(df)
    df = df.sort_values(["asset_id", "timestamp"]).groupby("asset_id").tail(1)

    pred_df = generate_predictions(model, feature_cols, df)
    store_predictions(client, pred_df)


if __name__ == "__main__":
    main()
