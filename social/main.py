from __future__ import annotations

import logging
import os
from typing import List

import pandas as pd

from gcp_utils import BigQueryClient
from .utils import load_config
from .x_scraper import XScraper
from .reddit_scraper import RedditScraper


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def run() -> None:
    config_path = os.getenv("SOCIAL_CONFIG_FILE", os.path.join(os.path.dirname(__file__), "config.yaml"))
    config = load_config(config_path)

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("BQ_SOCIAL_TABLE", "raw_social_posts")

    if not project_id or not dataset or not table:
        logging.error("Missing BigQuery configuration")
        return

    bq_client = BigQueryClient(project_id)
    bq_client.ensure_dataset_exists(dataset)

    posts: List[dict] = []

    bearer = os.getenv("TWITTER_BEARER_TOKEN")
    keywords = config.get("keywords", []) + [f"${c}" for c in config.get("cashtags", [])]
    if bearer and keywords:
        scraper = XScraper(keywords=keywords, bearer_token=bearer, max_results=int(os.getenv("TWITTER_MAX_RESULTS", "50")))
        posts.extend(scraper.fetch_posts())
    else:
        logging.warning("Twitter scraper not configured")

    reddit_id = os.getenv("REDDIT_CLIENT_ID")
    reddit_secret = os.getenv("REDDIT_CLIENT_SECRET")
    reddit_agent = os.getenv("REDDIT_USER_AGENT", "social-scraper")
    subreddits = config.get("subreddits", [])
    if reddit_id and reddit_secret and subreddits:
        r_scraper = RedditScraper(
            subreddits=subreddits,
            client_id=reddit_id,
            client_secret=reddit_secret,
            user_agent=reddit_agent,
            max_results=int(os.getenv("REDDIT_MAX_RESULTS", "50")),
        )
        posts.extend(r_scraper.fetch_posts())
    else:
        logging.warning("Reddit scraper not configured")

    if not posts:
        logging.info("No posts fetched")
        return

    df = pd.DataFrame(posts)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", unit="s")
    bq_client.upload_dataframe(df, dataset, table)
    logging.info("Uploaded %d posts", len(df))


if __name__ == "__main__":
    run()
