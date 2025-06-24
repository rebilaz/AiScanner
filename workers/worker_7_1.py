"""Compute sentiment metrics from social posts and community messages."""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from google.cloud import bigquery
from transformers import AutoModelForSequenceClassification, AutoTokenizer, TextClassificationPipeline
import nltk

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Download NLTK resources if not already present
try:
    nltk.data.find("tokenizers/punkt")
except LookupError:  # pragma: no cover - network required
    nltk.download("punkt")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SOCIAL_TABLE = os.getenv("BQ_SOCIAL_TABLE", "raw_social_posts")
COMMUNITY_TABLE = os.getenv("BQ_COMMUNITY_TABLE", "raw_community_messages")
DEST_TABLE = os.getenv("BQ_SENTIMENT_TABLE", "social_sentiment_timeseries")
MODEL_NAME = os.getenv("SENTIMENT_MODEL", "ProsusAI/finbert")
ASSET_CONFIG = os.getenv("ASSET_MAPPING_FILE")
BATCH_SIZE = int(os.getenv("SENTIMENT_BATCH_SIZE", "32"))

# Default mapping of cashtags to asset identifiers
DEFAULT_ASSET_MAP: Dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
}


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def load_asset_mapping() -> Dict[str, str]:
    if ASSET_CONFIG and os.path.exists(ASSET_CONFIG):
        try:
            import yaml  # local import to avoid global requirement

            with open(ASSET_CONFIG, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                return {k.upper(): str(v) for k, v in data.items()}
        except Exception:
            logging.exception("Failed to load asset mapping from %s", ASSET_CONFIG)
    return DEFAULT_ASSET_MAP


CLEAN_RE = re.compile(r"https?://\S+|@[A-Za-z0-9_]+", re.IGNORECASE)


def clean_text(text: str) -> str:
    """Remove URLs and user mentions, keep cashtags and emojis."""
    text = CLEAN_RE.sub("", text)
    return text.strip()


def extract_aspects(text: str, asset_map: Dict[str, str]) -> List[str]:
    """Return list of asset identifiers mentioned via cashtags."""
    matches = re.findall(r"\$([A-Za-z]{2,10})", text)
    assets = []
    for m in matches:
        asset = asset_map.get(m.upper())
        if asset:
            assets.append(asset)
    return assets


def load_messages(client: bigquery.Client, since_ts: Optional[str]) -> pd.DataFrame:
    """Load new messages from social and community tables."""
    conditions = ""
    if since_ts:
        conditions = f"WHERE timestamp > TIMESTAMP('{since_ts}')"

    query = f"""
        SELECT text, timestamp FROM `{client.project}.{DATASET}.{SOCIAL_TABLE}` {conditions}
        UNION ALL
        SELECT text, timestamp FROM `{client.project}.{DATASET}.{COMMUNITY_TABLE}` {conditions}
        ORDER BY timestamp
    """
    return client.query(query).to_dataframe()


def get_last_processed_timestamp(client: bigquery.Client) -> Optional[str]:
    """Return the latest processed timestamp from the destination table."""
    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    try:
        query = f"SELECT MAX(timestamp) AS ts FROM `{table_ref}`"
        df = client.query(query).to_dataframe()
        if df.empty or pd.isna(df.iloc[0]["ts"]):
            return None
        return str(df.iloc[0]["ts"])
    except Exception:
        logging.warning("Destination table %s not found, starting from scratch", table_ref)
        return None


def infer_sentiment(texts: Iterable[str], pipe: TextClassificationPipeline) -> List[str]:
    """Run sentiment inference on a batch of texts."""
    preds = pipe(list(texts), truncation=True)
    labels = []
    for p in preds:
        label = p["label"].lower()
        if "positive" in label or "bullish" in label:
            labels.append("positive")
        elif "negative" in label or "bearish" in label:
            labels.append("negative")
        else:
            labels.append("neutral")
    return labels


def aggregate_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sentiment counts per asset and hour."""
    if df.empty:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    grouped = df.groupby(["asset", pd.Grouper(key="timestamp", freq="1H")])
    agg = grouped["sentiment"].value_counts().unstack(fill_value=0).reset_index()
    agg = agg.rename(columns={"positive": "hype_score", "negative": "fud_score"})
    if "neutral" not in agg.columns:
        agg["neutral"] = 0
    agg["bull_bear_ratio"] = agg.apply(
        lambda r: r.get("hype_score", 0) / max(r.get("fud_score", 0), 1), axis=1
    )
    return agg[["asset", "timestamp", "hype_score", "fud_score", "bull_bear_ratio"]]


def fetch_previous_metrics(client: bigquery.Client, assets: List[str]) -> pd.DataFrame:
    """Fetch the latest metrics for given assets."""
    if not assets:
        return pd.DataFrame(columns=["asset", "timestamp", "hype_score", "fud_score"])

    asset_list = ", ".join(f"'{a}'" for a in assets)
    query = f"""
        WITH ranked AS (
            SELECT asset, timestamp, hype_score, fud_score,
                   ROW_NUMBER() OVER (PARTITION BY asset ORDER BY timestamp DESC) AS rn
            FROM `{client.project}.{DATASET}.{DEST_TABLE}`
            WHERE asset IN ({asset_list})
        )
        SELECT asset, timestamp, hype_score, fud_score
        FROM ranked WHERE rn = 1
    """
    return client.query(query).to_dataframe()


def compute_dynamics(agg: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    """Compute growth rates using previous metrics."""
    if agg.empty:
        return agg

    prev_map = {
        row["asset"]: row for _, row in prev.iterrows()
    }
    results: List[pd.Series] = []
    for asset, group in agg.sort_values("timestamp").groupby("asset"):
        last = prev_map.get(asset)
        for _, row in group.iterrows():
            if last is not None:
                row["hype_score_growth_rate"] = (row["hype_score"] - last["hype_score"]) / max(last["hype_score"], 1)
                row["fud_score_growth_rate"] = (row["fud_score"] - last["fud_score"]) / max(last["fud_score"], 1)
            else:
                row["hype_score_growth_rate"] = 0.0
                row["fud_score_growth_rate"] = 0.0
            last = row
            results.append(row)
    return pd.DataFrame(results)


def store_results(client: bigquery.Client, df: pd.DataFrame) -> None:
    """Append results to the destination table."""
    if df.empty:
        logging.info("No sentiment metrics to store")
        return
    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Uploading %d sentiment rows to %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


# ---------------------------------------------------------------------------
# Main worker
# ---------------------------------------------------------------------------

def run_social_sentiment_worker() -> None:
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    client = bigquery.Client(project=PROJECT_ID)
    asset_map = load_asset_mapping()

    last_ts = get_last_processed_timestamp(client)
    logging.info("Last processed timestamp: %s", last_ts)

    messages = load_messages(client, last_ts)
    if messages.empty:
        logging.info("No new messages to process")
        return

    messages["clean_text"] = messages["text"].astype(str).apply(clean_text)
    messages["assets"] = messages["clean_text"].apply(lambda t: extract_aspects(t, asset_map))
    messages = messages.explode("assets").dropna(subset=["assets"])
    if messages.empty:
        logging.info("No asset mentions found")
        return

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    pipe = TextClassificationPipeline(model=model, tokenizer=tokenizer, return_all_scores=False, device=-1)

    sentiments: List[str] = []
    texts_batch: List[str] = []
    for text in messages["clean_text"]:
        texts_batch.append(text)
        if len(texts_batch) >= BATCH_SIZE:
            sentiments.extend(infer_sentiment(texts_batch, pipe))
            texts_batch = []
    if texts_batch:
        sentiments.extend(infer_sentiment(texts_batch, pipe))

    messages["sentiment"] = sentiments
    agg = aggregate_scores(messages[["assets", "timestamp", "sentiment"]].rename(columns={"assets": "asset"}))

    prev = fetch_previous_metrics(client, list(agg["asset"].unique()))
    final_df = compute_dynamics(agg, prev)

    store_results(client, final_df)


if __name__ == "__main__":
    run_social_sentiment_worker()
