"""Fetch article URLs from RSS feeds."""

from __future__ import annotations

import logging
from typing import Iterable, List

import feedparser


class RSSFetcher:
    """Simple RSS feed fetcher."""

    def __init__(self, feeds: Iterable[str], max_entries: int = 50) -> None:
        self.feeds = list(feeds)
        self.max_entries = max_entries
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch_urls(self) -> List[str]:
        """Return article URLs from all configured feeds."""
        urls: List[str] = []
        for url in self.feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[: self.max_entries]:
                    link = getattr(entry, "link", None)
                    if link:
                        urls.append(link)
            except Exception:
                self.logger.exception("Failed to parse RSS feed %s", url)
        return urls
