"""Aggregate articles from RSS feeds, APIs, and direct URLs."""

from __future__ import annotations

import logging
import os
from typing import List, Dict

import pandas as pd
from google.cloud import bigquery

from gcp_utils import BigQueryClient
from .utils import load_config, article_exists
from .rss_fetcher import RSSFetcher
from .api_fetcher import NewsAPIFetcher
from .article_extractor import ArticleExtractor
from .storage import store_articles

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def run() -> None:
    config_path = os.getenv("NEWS_CONFIG_FILE", os.path.join(os.path.dirname(__file__), "config.yaml"))
    config = load_config(config_path)

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("BQ_ARTICLES_TABLE", "raw_articles")

    if not project_id or not dataset:
        logging.error("Missing BigQuery configuration")
        return

    bq_client = BigQueryClient(project_id)
    bq_client.ensure_dataset_exists(dataset)
    client = bq_client.client

    urls: List[str] = []

    rss_feeds = config.get("rss_feeds", [])
    if rss_feeds:
        fetcher = RSSFetcher(rss_feeds)
        urls.extend(fetcher.fetch_urls())

    for api_conf in config.get("api_sources", []):
        fetcher = NewsAPIFetcher(
            name=api_conf.get("name", "api"),
            url=api_conf.get("url", ""),
            params=api_conf.get("params"),
            headers=api_conf.get("headers"),
        )
        urls.extend(fetcher.fetch_urls())

    urls.extend(config.get("scrape_urls", []))

    urls = list(dict.fromkeys(urls))  # deduplicate while preserving order
    new_urls = [u for u in urls if not article_exists(client, dataset, table, u)]
    if not new_urls:
        logging.info("No new articles found")
        return

    extractor = ArticleExtractor()
    articles: List[Dict[str, str]] = []
    for url in new_urls:
        art = extractor.extract(url)
        if art:
            articles.append(art)

    store_articles(bq_client, dataset, table, articles)
    logging.info("Stored %d new articles", len(articles))


if __name__ == "__main__":
    run()
