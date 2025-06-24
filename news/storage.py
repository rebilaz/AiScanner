"""BigQuery storage utilities for articles."""

from __future__ import annotations

import logging
from typing import List, Dict

import pandas as pd
from google.cloud import bigquery

from gcp_utils import BigQueryClient


logger = logging.getLogger(__name__)


def store_articles(client: BigQueryClient, dataset: str, table: str, articles: List[Dict[str, str]]) -> None:
    """Upload articles to BigQuery."""
    if not articles:
        logger.info("No articles to store")
        return
    df = pd.DataFrame(articles)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    client.upload_dataframe(df, dataset, table)
