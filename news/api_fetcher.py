"""Fetch article URLs from news APIs."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests


class NewsAPIFetcher:
    """Generic news API fetcher expecting JSON with article URLs."""

    def __init__(self, name: str, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> None:
        self.name = name
        self.url = url
        self.params = params or {}
        self.headers = headers or {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch_urls(self) -> List[str]:
        """Call the API and return a list of article URLs."""
        try:
            resp = requests.get(self.url, params=self.params, headers=self.headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:  # pragma: no cover - network issues
            self.logger.exception("API request failed for %s", self.name)
            return []

        items = data.get("articles") or data.get("data") or []
        urls = [item.get("url") for item in items if isinstance(item, dict) and item.get("url")]
        return urls
