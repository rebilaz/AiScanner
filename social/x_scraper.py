from __future__ import annotations

import logging
from typing import List, Dict

import tweepy

from .base_scraper import BaseScraper


class XScraper(BaseScraper):
    """Scraper for X/Twitter using the v2 API."""

    def __init__(self, keywords: List[str], bearer_token: str, max_results: int = 50) -> None:
        super().__init__(max_results)
        self.keywords = keywords
        self.client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

    def fetch_posts(self) -> List[Dict[str, object]]:
        query = " OR ".join(self.keywords)
        logging.info("Searching Twitter for query: %s", query)
        try:
            resp = self.client.search_recent_tweets(
                query=query,
                max_results=min(self.max_results, 100),
                tweet_fields=["created_at", "public_metrics", "author_id"],
            )
        except Exception as exc:  # pragma: no cover - network issues
            logging.exception("Twitter API error: %s", exc)
            return []

        posts: List[Dict[str, object]] = []
        for tweet in resp.data or []:
            metrics = tweet.data.get("public_metrics", {}) if hasattr(tweet, "data") else tweet.public_metrics
            if metrics is None:
                metrics = {}
            engagement = metrics.get("like_count", 0) + metrics.get("retweet_count", 0)
            posts.append(
                {
                    "source": "twitter",
                    "author": str(getattr(tweet, "author_id", "")),
                    "text": tweet.text,
                    "timestamp": tweet.created_at.isoformat() if tweet.created_at else None,
                    "engagement_score": engagement,
                }
            )
        return posts
