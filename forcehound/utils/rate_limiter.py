"""Exponential-backoff decorator for Salesforce API rate-limit handling.

Salesforce enforces API request limits and will return HTTP 403 or specific
exception types when limits are exceeded.  The :func:`with_backoff` decorator
retries the wrapped function with exponential backoff and jitter.
"""

import asyncio
import functools
import logging
import random
from typing import Type, Tuple

logger = logging.getLogger(__name__)

# Exception types that should trigger a retry.
_RETRYABLE_STATUS_CODES = {429, 502, 503}


def with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """Decorator that retries an async function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts before giving up.
        base_delay: Initial delay in seconds before the first retry.
        max_delay: Upper-bound delay in seconds (caps exponential growth).
        retryable_exceptions: Tuple of exception types that trigger a retry.

    Returns:
        A decorator wrapping the target async function.

    Example::

        @with_backoff(max_retries=3, base_delay=2.0)
        async def call_salesforce(query: str) -> dict:
            ...
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except retryable_exceptions as exc:
                    last_exception = exc
                    if attempt == max_retries:
                        logger.error(
                            "All %d retries exhausted for %s: %s",
                            max_retries,
                            func.__name__,
                            exc,
                        )
                        raise

                    # Exponential backoff with full jitter.
                    delay = min(base_delay * (2**attempt), max_delay)
                    jittered = random.uniform(0, delay)
                    logger.warning(
                        "Retry %d/%d for %s after %.1fs: %s",
                        attempt + 1,
                        max_retries,
                        func.__name__,
                        jittered,
                        exc,
                    )
                    await asyncio.sleep(jittered)

            # Should never reach here, but satisfy type checkers.
            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator
