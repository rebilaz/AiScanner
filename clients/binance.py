import pandas as pd
from .base import AbstractCEXClient

BINANCE_BASE = "https://api.binance.com"

class BinanceClient(AbstractCEXClient):
    """Client for Binance public REST API."""

    def _format_pair(self, pair: str) -> str:
        return pair.replace("/", "").upper()

    async def get_ohlcv(self, pair: str, interval: str) -> pd.DataFrame | None:
        symbol = self._format_pair(pair)
        url = f"{BINANCE_BASE}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval}
        data = await self._request("GET", url, params=params)
        if data is None:
            return None
        cols = [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ]
        df = pd.DataFrame(data, columns=cols)
        df["event_timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df = df[["event_timestamp", "open", "high", "low", "close", "volume"]]
        df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
        return df

    async def get_order_book(self, pair: str, depth: int) -> pd.DataFrame | None:
        symbol = self._format_pair(pair)
        url = f"{BINANCE_BASE}/api/v3/depth"
        params = {"symbol": symbol, "limit": depth}
        data = await self._request("GET", url, params=params)
        if data is None:
            return None
        bids = pd.DataFrame(data.get("bids", []), columns=["price", "quantity"])
        asks = pd.DataFrame(data.get("asks", []), columns=["price", "quantity"])
        bids["side"] = "bid"
        asks["side"] = "ask"
        df = pd.concat([bids, asks])
        df = df.astype({"price": float, "quantity": float})
        return df

