import os
import aiohttp
import logging
from aiolimiter import AsyncLimiter

class CoinGeckoClient:
    """Minimal async client for the CoinGecko API."""

    def __init__(self, session: aiohttp.ClientSession, rate_limit: int | None = None) -> None:
        self.session = session
        if rate_limit is None:
            rate_limit = int(os.getenv("COINGECKO_RATE_LIMIT", "50"))
        # rate_limit is expressed per minute
        self.limiter = AsyncLimiter(rate_limit, 60)
        self.base = os.getenv("COINGECKO_API_BASE_URL", "https://api.coingecko.com/api/v3")

    async def _get(self, path: str, params: dict | None = None) -> dict | None:
        async with self.limiter:
            async with self.session.get(f"{self.base}{path}", params=params) as resp:
                body = await resp.text()
                if resp.status != 200:
                    logging.error(
                        f"[CoinGeckoClient] status={resp.status} on {path} | params={params} | body={body[:200]}"
                    )
                    return None
                try:
                    return await resp.json()
                except Exception as e:
                    logging.error(
                        f"[CoinGeckoClient] JSON parse error on {path}: {e} | body={body[:200]}"
                    )
                    return None

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
        if isinstance(data, list):
            return data
        return []

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
