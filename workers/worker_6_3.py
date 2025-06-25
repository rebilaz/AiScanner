"""Compute weighted asset ranking scores from multiple metrics."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

import pandas as pd
from google.cloud import bigquery
from gcp_utils import create_bq_client
from sklearn.preprocessing import MinMaxScaler
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
DEST_TABLE = os.getenv("BQ_ASSET_RANKING_TABLE", "asset_ranking_scores")
CONFIG_FILE = os.getenv(
    "SCORING_CONFIG_FILE",
    os.path.join(os.path.dirname(__file__), os.pardir, "scoring", "scoring_config.yaml"),
)

# Example SQL template. Adjust table names as needed.
SQL_TEMPLATE = """
SELECT
  asset,
  hype_score,
  hype_growth_rate,
  audit_score,
  bug_bounty_score,
  github_commits,
  contributor_count,
  liquidity_ratio,
  revenue_growth
FROM `{project}.{dataset}.asset_metrics`
"""

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def load_config(path: str) -> Dict[str, Any]:
    """Load scoring configuration from YAML."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        logging.error("Config file %s not found", path)
        return {}


def normalize_metrics(df: pd.DataFrame, metric_cols: List[str]) -> pd.DataFrame:
    """Normalize metric columns to 0-100 using MinMaxScaler."""
    if df.empty:
        return df
    scaler = MinMaxScaler(feature_range=(0, 100))
    df[metric_cols] = scaler.fit_transform(df[metric_cols])
    return df


def compute_sub_scores(df: pd.DataFrame, conf: Dict[str, Any]) -> pd.DataFrame:
    """Compute weighted sub-scores based on normalized metrics."""
    metrics_conf = conf.get("metrics", {})
    for sub_score, metrics in metrics_conf.items():
        total = 0.0
        for metric, weight in metrics.items():
            if metric in df.columns:
                total += df[metric] * float(weight)
        df[sub_score] = total
    return df


def compute_global_score(df: pd.DataFrame, conf: Dict[str, Any]) -> pd.DataFrame:
    """Compute global score from sub-scores."""
    weights = conf.get("global_weights", {})
    total = 0.0
    for sub_score, weight in weights.items():
        if sub_score in df.columns:
            total += df[sub_score] * float(weight)
    df["global_score"] = total
    df["rank"] = df["global_score"].rank(method="min", ascending=False)
    return df


def load_metrics(client: bigquery.Client) -> pd.DataFrame:
    """Execute SQL to fetch latest metrics for all assets."""
    sql = SQL_TEMPLATE.format(project=client.project, dataset=DATASET)
    return client.query(sql).to_dataframe()


def store_scores(client: bigquery.Client, df: pd.DataFrame) -> None:
    """Upload scoring results to BigQuery."""
    if df.empty:
        logging.info("No scores to store")
        return
    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    logging.info("Uploading %d ranking rows to %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


# ---------------------------------------------------------------------------
# Main worker
# ---------------------------------------------------------------------------

def run_asset_scoring_worker() -> None:
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    config = load_config(CONFIG_FILE)
    if not config:
        logging.error("Empty scoring configuration")
        return

    client = create_bq_client(PROJECT_ID)
    df = load_metrics(client)
    if df.empty:
        logging.info("No metrics found to score")
        return

    metric_cols = list(
        {metric for metrics in config.get("metrics", {}).values() for metric in metrics}
    )
    df = normalize_metrics(df, metric_cols)
    df = compute_sub_scores(df, config)
    df = compute_global_score(df, config)

    store_scores(client, df[["asset"] + metric_cols + list(config.get("metrics", {}).keys()) + ["global_score", "rank"]])


if __name__ == "__main__":
    run_asset_scoring_worker()
