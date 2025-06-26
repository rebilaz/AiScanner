import os
import aiohttp
import asyncio
import logging
from aiolimiter import AsyncLimiter

class CoinGeckoClient:
    """
    Asynchronous client for the CoinGecko API with built-in rate limiting
    and retry logic.
    """

    def __init__(self, session: aiohttp.ClientSession, rate_limit: int | None = None) -> None:
        self.session = session
        
        # Use provided rate_limit or get from environment, default to 10 reqs/minute
        if rate_limit is None:
            rate_limit = int(os.getenv("COINGECKO_RATE_LIMIT", "10"))
        
        # The limiter is configured per minute (60 seconds)
        self.limiter = AsyncLimiter(rate_limit, 60)
        self.base = os.getenv("COINGECKO_API_BASE_URL", "https://api.coingecko.com/api/v3")
        self.logger = logging.getLogger(__name__)

    async def get(self, path: str, params: dict | None = None, retries: int = 3) -> dict | list | None:
        """Perform GET request with exponential backoff retry logic."""
        backoff = 1
        url = f"{self.base}{path}"

        for attempt in range(retries):
            async with self.limiter:
                try:
                    async with self.session.get(url, params=params) as resp:
                        # Log successful request
                        if 200 <= resp.status < 300:
                            self.logger.debug(f"[CoinGeckoClient] status={resp.status} on {path}")
                            return await resp.json()

                        # Raise for non-retryable client errors immediately
                        if 400 <= resp.status < 500 and resp.status != 429:
                            body = await resp.text()
                            self.logger.error(f"[CoinGeckoClient] Client error status={resp.status} on {path} | body={body[:200]}")
                            resp.raise_for_status()

                        # For retryable errors (429, 5xx), let the retry logic handle it
                        if resp.status == 429 or 500 <= resp.status < 600:
                            self.logger.warning(f"Request failed with status {resp.status}. Retry {attempt + 1}/{retries} in {backoff}s...")
                            # This will trigger the exception below and the retry logic
                            resp.raise_for_status()

                except aiohttp.ClientResponseError as e:
                    if attempt == retries - 1: # If it's the last attempt, re-raise
                        raise
                    await asyncio.sleep(backoff)
                    backoff *= 2
                except Exception as e:
                    self.logger.error(f"An unexpected error occurred: {e}")
                    raise # Re-raise unexpected errors
        
        raise RuntimeError(f"Failed request to {url} after {retries} attempts")

    async def list_tokens(self, category: str) -> list[dict]:
        """Fetches a list of tokens for a given category."""
        params = {
            "vs_currency": "usd",
            "category": category,
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": "false",
        }
        data = await self.get("/coins/markets", params=params)
        return data if isinstance(data, list) else []

    # <-- CORRECTION FINALE APPLIQUÉE ICI
    async def token_details(self, token_id: str) -> dict | None:
        """Fetches detailed market data for a single token."""
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
        data = await self.get(f"/coins/{token_id}", params=params)
        return data if isinstance(data, dict) else None