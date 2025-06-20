import asyncio
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from clients.binance import BinanceClient


@pytest.mark.asyncio
async def test_get_ohlcv_normalization(monkeypatch):
    sample = [
        [1625097600000, "33000", "34000", "32000", "33500", "123.4", 1625097659999, "0", 0, "0", "0", "0"],
    ]
    client = BinanceClient()
    monkeypatch.setattr(client, "_request", AsyncMock(return_value=sample))

    df = await client.get_ohlcv("BTC/USDT", "1m")
    await client.close()

    assert not df.empty
    assert list(df.trading_pair.unique()) == ["BTC/USDT"]
    assert list(df.exchange_source.unique()) == ["Binance"]
    assert list(df.columns) == [
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
