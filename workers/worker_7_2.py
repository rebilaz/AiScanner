"""Generate topics from articles using BERTopic and store results in BigQuery."""

from __future__ import annotations

import logging
import os
from typing import Tuple

import pandas as pd
from google.cloud import bigquery
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
ARTICLE_TABLE = os.getenv("BQ_ARTICLES_TABLE", "raw_articles")
TOPIC_TABLE = os.getenv("BQ_TOPIC_TABLE", "topic_definitions")
TIMESERIES_TABLE = os.getenv("BQ_NARRATIVE_TABLE", "narrative_prevalence_timeseries")
LOOKBACK_DAYS = int(os.getenv("TOPIC_LOOKBACK_DAYS", "90"))
MIN_TOPIC_SIZE = int(os.getenv("TOPIC_MIN_SIZE", "10"))
EMBED_MODEL = os.getenv("TOPIC_EMBED_MODEL", "all-MiniLM-L6-v2")

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def fetch_articles(client: bigquery.Client) -> pd.DataFrame:
    """Return recent articles for modeling."""
    query = f"""
        SELECT url, title, text, CAST(date AS DATE) AS date
        FROM `{PROJECT_ID}.{DATASET}.{ARTICLE_TABLE}`
        WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
          AND text IS NOT NULL
    """
    logging.info("Fetching articles with query: %s", query)
    job = client.query(query)
    df = job.result().to_dataframe()
    logging.info("Fetched %d articles", len(df))
    return df


def train_topic_model(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Train BERTopic and return assignments and topic info."""
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    embed_model = SentenceTransformer(EMBED_MODEL)
    topic_model = BERTopic(embedding_model=embed_model, min_topic_size=MIN_TOPIC_SIZE)
    topics, _ = topic_model.fit_transform(df["text"].tolist())

    assignments = pd.DataFrame({
        "url": df["url"],
        "topic_id": topics,
        "date": pd.to_datetime(df["date"], errors="coerce"),
    })
    topic_info = topic_model.get_topic_info()
    topic_info = topic_info.rename(columns={"Topic": "topic_id", "Name": "name", "Count": "document_count"})
    return assignments, topic_info


def store_dataframe(client: bigquery.Client, df: pd.DataFrame, table: str) -> None:
    """Upload DataFrame to a BigQuery table."""
    if df.empty:
        logging.info("No data to store for %s", table)
        return
    table_ref = f"{client.project}.{DATASET}.{table}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Uploading %d rows to %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


def compute_prevalence(assignments: pd.DataFrame) -> pd.DataFrame:
    """Compute weekly prevalence of each topic."""
    if assignments.empty:
        return assignments
    df = assignments.copy()
    df["week_timestamp"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)
    ts = (
        df.groupby(["topic_id", "week_timestamp"]).size().reset_index(name="article_count")
    )
    return ts


async def run_topic_modeler_worker() -> None:
    """Main worker entrypoint."""
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    client = bigquery.Client(project=PROJECT_ID)

    articles = fetch_articles(client)
    if articles.empty:
        logging.info("No articles to process")
        return

    assignments, topic_info = train_topic_model(articles)
    if topic_info.empty:
        logging.info("No topics generated")
        return

    prevalence = compute_prevalence(assignments)

    store_dataframe(client, topic_info, TOPIC_TABLE)
    store_dataframe(client, prevalence, TIMESERIES_TABLE)


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_topic_modeler_worker())
