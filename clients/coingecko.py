<<<<<<< ifkvoe-codex/analyser-blocage-api-coingecko-et-solutions
import os
import aiohttp
import asyncio
import logging
from aiolimiter import AsyncLimiter
=======
import os
import aiohttp
import asyncio
import logging
from aiolimiter import AsyncLimiter
>>>>>>> main

class CoinGeckoClient:
    """Minimal async client for the CoinGecko API."""

    def __init__(self, session: aiohttp.ClientSession, rate_limit: int | None = None) -> None:
        self.session = session
        if rate_limit is None:
            rate_limit = int(os.getenv("COINGECKO_RATE_LIMIT", "50"))
        # rate_limit is expressed per minute
        self.limiter = AsyncLimiter(rate_limit, 60)
        self.base = os.getenv("COINGECKO_API_BASE_URL", "https://api.coingecko.com/api/v3")

<<<<<<< ifkvoe-codex/analyser-blocage-api-coingecko-et-solutions
    async def _get(self, path: str, params: dict | None = None, retries: int = 3) -> dict | None:
        """Perform GET request with basic retry logic."""
        backoff = 1
        url = f"{self.base}{path}"
        for attempt in range(retries):
            async with self.limiter:
                try:
                    async with self.session.get(url, params=params) as resp:
                        body = await resp.text()
                        if resp.status == 429 or 500 <= resp.status < 600:
                            raise aiohttp.ClientResponseError(
                                resp.request_info,
                                resp.history,
                                status=resp.status,
                                message=body,
                                headers=resp.headers,
                            )
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
                except aiohttp.ClientResponseError as e:
                    if e.status == 429 or 500 <= e.status < 600:
                        logging.warning(
                            "Request failed with status %s. Retry %s/%s in %ss",
                            e.status,
                            attempt + 1,
                            retries,
                            backoff,
                        )
                        await asyncio.sleep(backoff)
                        backoff *= 2
                    else:
                        raise
        raise RuntimeError(f"Failed request to {url} after {retries} attempts")
=======
    def __init__(self, session: aiohttp.ClientSession, rate_limit: int | None = None) -> None:
        self.session = session
        if rate_limit is None:
            rate_limit = int(os.getenv("COINGECKO_RATE_LIMIT", "50"))
        # rate_limit is expressed per minute
        self.limiter = AsyncLimiter(rate_limit, 60)
        self.base = os.getenv("COINGECKO_API_BASE_URL", "https://api.coingecko.com/api/v3")


    async def _get(self, path: str, params: dict | None = None, retries: int = 3) -> dict | None:
        """Perform GET request with basic retry logic."""
        backoff = 1
        url = f"{self.base}{path}"
        for attempt in range(retries):
            async with self.limiter:
                try:
                    async with self.session.get(url, params=params) as resp:
                        body = await resp.text()
                        if resp.status == 429 or 500 <= resp.status < 600:
                            raise aiohttp.ClientResponseError(
                                resp.request_info,
                                resp.history,
                                status=resp.status,
                                message=body,
                                headers=resp.headers,
                            )
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
                except aiohttp.ClientResponseError as e:
                    if e.status == 429 or 500 <= e.status < 600:
                        logging.warning(
                            "Request failed with status %s. Retry %s/%s in %ss",
                            e.status,
                            attempt + 1,
                            retries,
                            backoff,
                        )
                        await asyncio.sleep(backoff)
                        backoff *= 2
                    else:
                        raise
        raise RuntimeError(f"Failed request to {url} after {retries} attempts")

>>>>>>> main

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
