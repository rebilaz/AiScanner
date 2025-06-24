import abc
import asyncio
from aiolimiter import AsyncLimiter
import aiohttp
import pandas as pd
import logging
from typing import Optional

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None

class AbstractCEXClient(abc.ABC):
    """Abstract base class for CEX REST API clients."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        rate_limit: int = 5,
        proxy: str | None = None,
        browser_ws: str | None = None,
    ):
        self.session = session
        self.limiter = AsyncLimiter(rate_limit, 1)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.proxy = proxy
        self.browser_ws = browser_ws

    async def _fetch_via_browser(
        self, method: str, url: str, params: dict | None = None
    ) -> dict | None:
        """Attempt to fetch a URL using a remote browser via Playwright."""
        if async_playwright is None or not self.browser_ws:
            return None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp(self.browser_ws)
                context = await browser.new_context()
                request = context.request
                if method.upper() == "GET":
                    response = await request.get(url, params=params)
                else:
                    response = await request.post(url, data=params)
                if response.status >= 400:
                    return None
                return await response.json()
        except Exception as exc:  # pragma: no cover - best effort fallback
            self.logger.error("Browser fetch failed: %s", exc)
            return None

    async def _request(
        self, method: str, url: str, params: dict | None = None, retries: int = 3
    ) -> dict | None:
        backoff = 1
        for attempt in range(retries):
            async with self.limiter:
                try:
                    async with self.session.request(
                        method,
                        url,
                        params=params,
                        proxy=self.proxy,
                    ) as resp:
                        if resp.status == 429 or 500 <= resp.status < 600:
                            raise aiohttp.ClientResponseError(
                                resp.request_info,
                                resp.history,
                                status=resp.status,
                                message=await resp.text(),
                                headers=resp.headers,
                            )
                        resp.raise_for_status()
                        return await resp.json()
                except aiohttp.ClientResponseError as e:
                    if e.status == 429 or 500 <= e.status < 600:
                        self.logger.warning(
                            "Request failed with status %s. Retry %s/%s in %ss",
                            e.status,
                            attempt + 1,
                            retries,
                            backoff,
                        )
                        await asyncio.sleep(backoff)
                        backoff *= 2
                    elif e.status == 451:
                        logging.error(
                            f"Acc\u00e8s refus\u00e9 pour {self.__class__.__name__} en raison d'une restriction g\u00e9ographique (Erreur 451)."
                        )
                        via_browser = await self._fetch_via_browser(method, url, params)
                        if via_browser is not None:
                            self.logger.info("Retrieved data via browser fallback")
                            return via_browser
                        self.logger.error(
                            "Ce client sera d\u00e9sactiv\u00e9 car la r\u00e9cup\u00e9ration via navigateur a \u00e9chou\u00e9."
                        )
                        return None
                    else:
                        raise
        raise RuntimeError(f"Failed request to {url} after {retries} attempts")

    @abc.abstractmethod
    async def get_ohlcv(self, pair: str, interval: str) -> Optional[pd.DataFrame]:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_order_book(self, pair: str, depth: int) -> Optional[pd.DataFrame]:
        raise NotImplementedError
