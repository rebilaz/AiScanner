import pytest
from dex_clients.thegraph import TheGraphClient


@pytest.mark.asyncio
async def test_fetch_swaps_pagination(mocker):
    first_page = {"swaps": [{"transaction": {"id": "1"}, "timestamp": "1", "pool": {"token0": {"symbol": "A"}, "token1": {"symbol": "B"}}, "amount0": "1", "amount1": "-1", "amountUSD": "1", "sqrtPriceX96": "0"}]}
    second_page = {"swaps": []}

    execute_mock = mocker.AsyncMock(side_effect=[first_page, second_page])
    session = mocker.MagicMock(execute=execute_mock)
    cm = mocker.MagicMock()
    cm.__aenter__ = mocker.AsyncMock(return_value=session)
    cm.__aexit__ = mocker.AsyncMock(return_value=None)

    mocker.patch("dex_clients.thegraph.AIOHTTPTransport")
    mocker.patch("dex_clients.thegraph.Client", return_value=cm)

    client = TheGraphClient("http://example.com")
    swaps = await client.fetch_swaps(0, 10)

    assert swaps == first_page["swaps"]
    execute_mock.assert_called()

