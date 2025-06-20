"""Utility helpers for the CEX worker."""
from __future__ import annotations

import asyncio
import logging
from functools import wraps
from typing import Any, Callable, Iterable


def retry_with_backoff(
    retries: int = 3,
    base_delay: float = 1.0,
    exceptions: Iterable[type[Exception]] | tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator implementing exponential backoff retries."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    attempt += 1
                    if attempt > retries:
                        logging.error("Max retries exceeded for %s: %s", func.__name__, exc)
                        raise
                    delay = base_delay * 2 ** (attempt - 1)
                    logging.warning(
                        "%s failed (%s). Retrying in %.1fs (%d/%d)",
                        func.__name__,
                        exc,
                        delay,
                        attempt,
                        retries,
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator


def rate_limited(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator applying the client's AsyncLimiter."""

    @wraps(func)
    async def wrapper(self, *args: Any, **kwargs: Any) -> Any:
        async with self.limiter:
            return await func(self, *args, **kwargs)

    return wrapper
