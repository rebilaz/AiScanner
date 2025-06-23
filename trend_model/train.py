"""Train an XGBoost model to predict 7‑day price increases for crypto assets."""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import List, Tuple

import joblib
import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.metrics import f1_score, roc_auc_score
from xgboost import XGBClassifier
import ta

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SOURCE_TABLE = os.getenv("BQ_PRICE_FEATURES_TABLE", "asset_price_features")
MODEL_PATH = os.getenv("PRICE_MODEL_PATH", os.path.join(os.path.dirname(__file__), "model.pkl"))

# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def create_features(df: pd.DataFrame) -> pd.DataFrame:
    """Generate technical indicators and lag features."""
    if df.empty:
        return df

    df = df.sort_values(["asset_id", "timestamp"]).copy()

    def add_indicators(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        group["sma_7"] = ta.trend.sma_indicator(group["close"], window=7)
        group["sma_30"] = ta.trend.sma_indicator(group["close"], window=30)
        group["rsi_14"] = ta.momentum.rsi(group["close"], window=14)
        macd = ta.trend.macd(group["close"])
        group["macd"] = macd
        group["return_1d"] = group["close"].pct_change() * 100
        group["volume_change"] = group["volume"].pct_change() * 100
        group["sentiment_lag1"] = group["sentiment"].shift(1)
        group["close_lag1"] = group["close"].shift(1)
        return group

    df = df.groupby("asset_id", group_keys=False).apply(add_indicators)
    df = df.dropna()
    return df

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_training_data(client: bigquery.Client) -> pd.DataFrame:
    """Fetch aggregated features and target from BigQuery."""
    sql = f"""
        SELECT *
        FROM `{client.project}.{DATASET}.{SOURCE_TABLE}`
    """
    df = client.query(sql).to_dataframe()
    return df

# ---------------------------------------------------------------------------
# Target creation
# ---------------------------------------------------------------------------

def build_target(df: pd.DataFrame) -> pd.DataFrame:
    """Create binary target based on future 7‑day price change."""
    df = df.sort_values(["asset_id", "timestamp"]).copy()

    def label(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        future_price = group["close"].shift(-7)
        group["target"] = ((future_price - group["close"]) / group["close"] > 0.05).astype(int)
        return group

    df = df.groupby("asset_id", group_keys=False).apply(label)
    df = df.dropna(subset=["target"])  # drop last rows without future price
    return df

# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------

def train_model(df: pd.DataFrame, feature_cols: List[str]) -> Tuple[XGBClassifier, float, float]:
    """Train XGBClassifier and return model and metrics."""
    df = df.sort_values("timestamp")
    split_point = int(len(df) * 0.8)
    train_df = df.iloc[:split_point]
    valid_df = df.iloc[split_point:]

    model = XGBClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=4,
    )

    model.fit(train_df[feature_cols], train_df["target"])
    probs = model.predict_proba(valid_df[feature_cols])[:, 1]
    preds = (probs >= 0.5).astype(int)
    auc = roc_auc_score(valid_df["target"], probs)
    f1 = f1_score(valid_df["target"], preds)
    return model, auc, f1

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_model(model: XGBClassifier, feature_cols: List[str]) -> None:
    """Persist model and feature list to disk."""
    joblib.dump({"model": model, "features": feature_cols}, MODEL_PATH)
    logging.info("Model saved to %s", MODEL_PATH)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    client = bigquery.Client(project=PROJECT_ID)
    df = load_training_data(client)
    if df.empty:
        logging.error("Training data is empty")
        return

    df = create_features(df)
    df = build_target(df)

    feature_cols = [
        c
        for c in df.columns
        if c
        not in ["asset_id", "timestamp", "close", "target"]
    ]

    model, auc, f1 = train_model(df, feature_cols)
    logging.info("Validation AUC: %.4f F1: %.4f", auc, f1)

    save_model(model, feature_cols)


if __name__ == "__main__":
    main()
