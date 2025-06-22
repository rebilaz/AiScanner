import os
import aiohttp
from aiolimiter import AsyncLimiter


class CoinGeckoClient:
    """Minimal async client for the CoinGecko API."""

    def __init__(self, session: aiohttp.ClientSession, rate_limit: int = 10) -> None:
        self.session = session
        self.limiter = AsyncLimiter(rate_limit, 1)
        self.base = os.getenv("COINGECKO_API_BASE_URL", "https://api.coingecko.com/api/v3")

    async def _get(self, path: str, params: dict | None = None) -> dict | None:
        async with self.limiter:
            async with self.session.get(f"{self.base}{path}", params=params) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()

    async def list_tokens(self, category: str) -> list[dict] | None:
        params = {
            "vs_currency": "usd",
            "category": category,
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": "false",
        }
        data = await self._get("/coins/markets", params)
        return data or []

    async def token_details(self, token_id: str) -> dict | None:
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
        return await self._get(f"/coins/{token_id}", params)
