"""Extract article content from a web page using trafilatura."""

from __future__ import annotations

import json
import logging
from typing import Optional, Dict
from urllib.parse import urlparse

import requests
import trafilatura


class ArticleExtractor:
    """Download and parse articles."""

    def __init__(self, user_agent: str | None = None) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent or "Mozilla/5.0"})
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract(self, url: str) -> Optional[Dict[str, str]]:
        """Return a structured article dict or None on failure."""
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
        except Exception:  # pragma: no cover - network issues
            self.logger.exception("Failed to download %s", url)
            return None

        processed = trafilatura.extract(
            resp.text,
            output_format="json",
            with_metadata=True,
            include_comments=False,
            include_tables=False,
        )
        if not processed:
            self.logger.warning("Extraction failed for %s", url)
            return None

        data = json.loads(processed)
        return {
            "url": url,
            "source": urlparse(url).netloc,
            "title": data.get("title"),
            "author": data.get("author"),
            "date": data.get("date"),
            "text": data.get("text"),
        }
