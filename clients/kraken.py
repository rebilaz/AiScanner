import pandas as pd
from .base import AbstractCEXClient

KRAKEN_BASE = "https://api.kraken.com"

class KrakenClient(AbstractCEXClient):
    """Client for Kraken public REST API."""
    INTERVAL_MAP = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "1h": 60,
        "4h": 240,
        "1d": 1440,
        "1w": 10080,
        "1M": 43200,   
    }
    def _format_pair(self, pair: str) -> str:
        base, quote = pair.split("/")
        if base.upper() == "BTC":
            base = "XBT"
        return f"{base}{quote}".upper()

    async def get_ohlcv(self, pair: str, interval: str) -> pd.DataFrame | None:
        symbol = self._format_pair(pair)
        url = f"{KRAKEN_BASE}/0/public/OHLC"
        params = {"pair": symbol, "interval": self.INTERVAL_MAP.get(interval)}
        data = await self._request("GET", url, params=params)
        if data is None:
            return None
        
        if data.get("error"):
            self.logger.error(f"Kraken API a retourné une erreur : {data['error']}")
            return None  
        
        key = next(iter(data["result"].keys()))
        ohlc = data["result"][key]
        df = pd.DataFrame(ohlc, columns=[
            "time", "open", "high", "low", "close", "vwap", "volume", "count"
        ])
        df["event_timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df[["event_timestamp", "open", "high", "low", "close", "volume"]]
        df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
        return df

    async def get_order_book(self, pair: str, depth: int) -> pd.DataFrame | None:
        symbol = self._format_pair(pair)
        url = f"{KRAKEN_BASE}/0/public/Depth"
        params = {"pair": symbol, "count": depth}
        data = await self._request("GET", url, params=params)
        if data is None:
            return None
        key = next(iter(data["result"].keys()))
        book = data["result"][key]
        bids = pd.DataFrame(book.get("bids", []), columns=["price", "quantity", "timestamp"])
        asks = pd.DataFrame(book.get("asks", []), columns=["price", "quantity", "timestamp"])
        bids["side"] = "bid"
        asks["side"] = "ask"
        df = pd.concat([bids, asks])
        df = df.drop(columns=["timestamp"], errors="ignore")
        df = df.astype({"price": float, "quantity": float})
        return df

