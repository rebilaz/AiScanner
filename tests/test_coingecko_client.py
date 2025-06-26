import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import asyncio
import aiohttp
import pytest

from clients.coingecko import CoinGeckoClient


@pytest.mark.asyncio
async def test_get_coin_details_success(mocker):
    mock_session = mocker.MagicMock(spec=aiohttp.ClientSession)
    mock_resp = mocker.MagicMock()
    mock_resp.status = 200
    mock_resp.json = mocker.AsyncMock(return_value={"id": "btc"})
    mock_resp.text = mocker.AsyncMock(return_value="{}")
    cm = mocker.MagicMock()
    cm.__aenter__ = mocker.AsyncMock(return_value=mock_resp)
    cm.__aexit__ = mocker.AsyncMock(return_value=None)
    mock_session.get.return_value = cm
    client = CoinGeckoClient(mock_session, rate_limit=100)

    data = await client.token_details("bitcoin")
    assert data == {"id": "btc"}


@pytest.mark.asyncio
async def test_get_coin_details_rate_limit_retry(mocker):
    mock_session = mocker.MagicMock(spec=aiohttp.ClientSession)
    resp_429 = mocker.MagicMock()
    resp_429.status = 429
    resp_429.text = mocker.AsyncMock(return_value="")
    resp_429.json = mocker.AsyncMock(return_value={})
    resp_ok = mocker.MagicMock()
    resp_ok.status = 200
    resp_ok.text = mocker.AsyncMock(return_value="{}")
    resp_ok.json = mocker.AsyncMock(return_value={"id": "btc"})

    cm_429 = mocker.MagicMock()
    cm_429.__aenter__ = mocker.AsyncMock(return_value=resp_429)
    cm_429.__aexit__ = mocker.AsyncMock(return_value=None)
    cm_ok = mocker.MagicMock()
    cm_ok.__aenter__ = mocker.AsyncMock(return_value=resp_ok)
    cm_ok.__aexit__ = mocker.AsyncMock(return_value=None)

    mock_session.get.side_effect = [cm_429, cm_ok]
    client = CoinGeckoClient(mock_session, rate_limit=100)

    mocker.patch("asyncio.sleep", mocker.AsyncMock())
    data = await client.token_details("bitcoin")
    assert data == {"id": "btc"}
    assert mock_session.get.call_count == 2


@pytest.mark.asyncio
async def test_get_coin_details_server_error(mocker):
    mock_session = mocker.MagicMock(spec=aiohttp.ClientSession)
    resp_503 = mocker.MagicMock()
    resp_503.status = 503
    resp_503.text = mocker.AsyncMock(return_value="")
    resp_503.json = mocker.AsyncMock(return_value={})
    cm = mocker.MagicMock()
    cm.__aenter__ = mocker.AsyncMock(return_value=resp_503)
    cm.__aexit__ = mocker.AsyncMock(return_value=None)
    mock_session.get.return_value = cm

    client = CoinGeckoClient(mock_session, rate_limit=100)
    mocker.patch("asyncio.sleep", mocker.AsyncMock())
    with pytest.raises(RuntimeError):
        await client.token_details("bitcoin")


@pytest.mark.asyncio
async def test_get_coin_details_invalid_json(mocker):
    mock_session = mocker.MagicMock(spec=aiohttp.ClientSession)
    resp = mocker.MagicMock()
    resp.status = 200
    resp.text = mocker.AsyncMock(return_value="not json")
    resp.json = mocker.AsyncMock(side_effect=ValueError("invalid"))
    cm = mocker.MagicMock()
    cm.__aenter__ = mocker.AsyncMock(return_value=resp)
    cm.__aexit__ = mocker.AsyncMock(return_value=None)
    mock_session.get.return_value = cm

    client = CoinGeckoClient(mock_session, rate_limit=100)
    data = await client.token_details("bitcoin")
    assert data is None
