import os
import aiohttp
import asyncio
import logging
from aiolimiter import AsyncLimiter

class CoinGeckoClient:
    def __init__(self, session: aiohttp.ClientSession, rate_limit: int | None = None) -> None:
        self.session = session
        if rate_limit is None:
            rate_limit = int(os.getenv("COINGECKO_RATE_LIMIT", "10"))
        self.limiter = AsyncLimiter(rate_limit, 60)
        self.base = os.getenv("COINGECKO_API_BASE_URL", "https://api.coingecko.com/api/v3")
        self.api_key = os.getenv("COINGECKO_API_KEY", "")
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"RATE LIMIT EFFECTIVE: {rate_limit} req/min")
        if self.api_key and "pro-api" not in self.base:
            self.logger.warning("Clé API présente mais URL base non PRO !")
        if not self.api_key and "pro-api" in self.base:
            self.logger.warning("URL PRO mais pas de clé API fournie !")

    async def get(self, path: str, params: dict | None = None, retries: int = 5) -> dict | list | None:
        backoff = 1
        url = f"{self.base}{path}"
        last_status = None
        for attempt in range(retries):
            async with self.limiter:
                self.logger.info(f"[CoinGeckoClient] GET {url} | params={params}")
                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0 (compatible; CoinGeckoClient/1.0; +https://tonsite.com)"
                    }
                    if self.api_key and "pro-api" in self.base:
                        headers["x-cg-pro-api-key"] = self.api_key
                    async with self.session.get(url, params=params, headers=headers) as resp:
                        last_status = resp.status
                        if 200 <= resp.status < 300:
                            try:
                                return await resp.json()
                            except Exception as e:
                                self.logger.error(f"[CoinGeckoClient] JSON decode error: {e}")
                                return None
                        if resp.status == 429:
                            self.logger.warning(f"[CoinGeckoClient] 429 headers: {dict(resp.headers)}")
                            self.logger.warning(f"[CoinGeckoClient] 429 body: {await resp.text()}")
                            retry_after = resp.headers.get("Retry-After")
                            if retry_after is not None:
                                try:
                                    sleep_time = float(retry_after)
                                    self.logger.warning(f"[CoinGeckoClient] 429 received. Waiting {sleep_time}s (Retry-After header). Attempt {attempt+1}/{retries}.")
                                    await asyncio.sleep(sleep_time)
                                except Exception:
                                    self.logger.warning(f"[CoinGeckoClient] 429 received. Retry-After not float: {retry_after}. Defaulting to {backoff}s.")
                                    await asyncio.sleep(backoff)
                            else:
                                self.logger.warning(f"[CoinGeckoClient] 429 received. No Retry-After header. Defaulting to {backoff}s.")
                                await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 120)
                            continue
                        if 500 <= resp.status < 600:
                            self.logger.warning(f"[CoinGeckoClient] 5xx error: {resp.status}. Retrying in {backoff}s (Attempt {attempt+1}/{retries})...")
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 120)
                            continue
                        if 400 <= resp.status < 500:
                            body = await resp.text()
                            self.logger.error(f"[CoinGeckoClient] Client error status={resp.status} on {path} | body={body[:200]}")
                            return None
                except aiohttp.ClientResponseError as e:
                    self.logger.error(f"[CoinGeckoClient] ClientResponseError {e.status}: {e}")
                    if attempt == retries - 1:
                        raise
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 120)
                except Exception as e:
                    self.logger.error(f"[CoinGeckoClient] Unexpected error: {e}")
                    if attempt == retries - 1:
                        raise
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 120)
        raise RuntimeError(f"Failed request to {url} after {retries} attempts, last status={last_status}")

    async def list_tokens(self, category: str) -> list[dict]:
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

    async def token_details(self, token_id: str) -> dict | None:
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
