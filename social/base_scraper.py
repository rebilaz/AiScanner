from __future__ import annotations

import abc
from typing import List, Dict


class BaseScraper(abc.ABC):
    """Abstract base class for social media scrapers."""

    def __init__(self, max_results: int = 50) -> None:
        self.max_results = max_results

    @abc.abstractmethod
    def fetch_posts(self) -> List[Dict[str, object]]:
        """Return a list of standardized post dictionaries."""
        raise NotImplementedError
