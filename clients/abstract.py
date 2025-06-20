"""Abstract client definitions for centralized exchanges."""
from __future__ import annotations

import abc
import aiohttp
from aiolimiter import AsyncLimiter
import pandas as pd


class AbstractCEXClient(abc.ABC):
    """Base class for CEX API clients."""

    def __init__(self, rate_limit: tuple[int, int] | None = None) -> None:
        self.session = aiohttp.ClientSession()
        self.limiter = AsyncLimiter(*(rate_limit or (60, 1)))

    @abc.abstractmethod
    async def get_ohlcv(self, trading_pair: str, interval: str) -> pd.DataFrame:
        """Return OHLCV data for a trading pair."""

    @abc.abstractmethod
    async def get_order_book(self, trading_pair: str, depth: int = 20) -> dict:
        """Return order book snapshot."""

    async def close(self) -> None:
        """Close underlying HTTP session."""
        await self.session.close()
