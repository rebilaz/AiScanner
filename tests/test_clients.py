import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import pytest
import aiohttp
from clients.binance import BinanceClient


@pytest.mark.asyncio
async def test_binance_get_ohlcv(mocker):
    sample_response = [
        [
            1625097600000,
            "1",
            "2",
            "0.5",
            "1.5",
            "100",
            1625097659999,
            "1",
            "100",
            "50",
            "25",
            "0"
        ]
    ]

    mock_session = mocker.MagicMock(spec=aiohttp.ClientSession)
    mock_resp = mocker.MagicMock()
    mock_resp.status = 200
    mock_resp.json = mocker.AsyncMock(return_value=sample_response)
    mock_resp.text = mocker.AsyncMock(return_value="")

    cm = mocker.MagicMock()
    cm.__aenter__ = mocker.AsyncMock(return_value=mock_resp)
    cm.__aexit__ = mocker.AsyncMock(return_value=None)
    mock_session.request.return_value = cm

    client = BinanceClient(mock_session, rate_limit=10)

    df = await client.get_ohlcv("BTC/USDT", "1m")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == [
        "event_timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert len(df) == len(sample_response)
    assert pd.api.types.is_datetime64_any_dtype(df["event_timestamp"])
    for col in ["open", "high", "low", "close", "volume"]:
        assert pd.api.types.is_float_dtype(df[col])
