from __future__ import annotations

import logging
from typing import List, Dict

import praw

from .base_scraper import BaseScraper


class RedditScraper(BaseScraper):
    """Scraper for Reddit posts in specified subreddits."""

    def __init__(
        self,
        subreddits: List[str],
        client_id: str,
        client_secret: str,
        user_agent: str,
        max_results: int = 50,
    ) -> None:
        super().__init__(max_results)
        self.subreddits = subreddits
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )

    def fetch_posts(self) -> List[Dict[str, object]]:
        posts: List[Dict[str, object]] = []
        for name in self.subreddits:
            logging.info("Fetching posts from subreddit: %s", name)
            try:
                subreddit = self.reddit.subreddit(name)
                for submission in subreddit.new(limit=self.max_results):
                    posts.append(
                        {
                            "source": "reddit",
                            "author": submission.author.name if submission.author else None,
                            "text": f"{submission.title}\n{submission.selftext}",
                            "timestamp": submission.created_utc,
                            "engagement_score": submission.score,
                        }
                    )
            except Exception as exc:  # pragma: no cover - network issues
                logging.exception("Error reading subreddit %s: %s", name, exc)
        return posts
