"""Binance client implementation."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict

import pandas as pd
import aiohttp

from .abstract import AbstractCEXClient
from utils import rate_limited, retry_with_backoff


class BinanceClient(AbstractCEXClient):
    """Client for Binance public REST API."""

    BASE_URL = "https://api.binance.com"

    def __init__(self, rate_limit: tuple[int, int] | None = (1200, 60)) -> None:
        super().__init__(rate_limit=rate_limit)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _format_pair(self, trading_pair: str) -> str:
        return trading_pair.replace("/", "").upper()

    @rate_limited
    @retry_with_backoff(retries=5, base_delay=1)
    async def _request(self, path: str, params: Dict[str, Any]) -> Any:
        url = f"{self.BASE_URL}{path}"
        async with self.session.get(url, params=params, timeout=10) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise aiohttp.ClientError(f"HTTP {resp.status}: {text}")
            return await resp.json()

    @retry_with_backoff(retries=3)
    async def get_ohlcv(self, trading_pair: str, interval: str) -> pd.DataFrame:
        """Fetch OHLCV data as DataFrame."""
        symbol = self._format_pair(trading_pair)
        data = await self._request("/api/v3/klines", {"symbol": symbol, "interval": interval})

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(
            data,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base",
                "taker_buy_quote",
                "ignore",
            ],
        )

        df["ingestion_timestamp"] = datetime.utcnow()
        df["event_timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
        df["trading_pair"] = trading_pair
        df["exchange_source"] = "Binance"
        df["granularity"] = interval

        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].astype(float)

        return df[
            [
                "ingestion_timestamp",
                "event_timestamp",
                "trading_pair",
                "exchange_source",
                "granularity",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        ]

    @retry_with_backoff(retries=3)
    async def get_order_book(self, trading_pair: str, depth: int = 20) -> Dict[str, Any]:
        symbol = self._format_pair(trading_pair)
        data = await self._request("/api/v3/depth", {"symbol": symbol, "limit": depth})
        data["ingestion_timestamp"] = datetime.utcnow().isoformat()
        data["trading_pair"] = trading_pair
        data["exchange_source"] = "Binance"
        return data
