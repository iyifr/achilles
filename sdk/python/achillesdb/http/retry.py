from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Callable, Optional, TypeVar
from ..errors import ERROR_CONNECTION, AchillesError

T = TypeVar("T")

RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "DELETE", "OPTIONS"})

logger = logging.getLogger(__name__)


def _backoff(attempt: int, retry_after: Optional[float]) -> float:
    """Return how long to sleep before the next attempt."""
    if retry_after is not None:
        return retry_after
    ceiling = min(0.5 * (2 ** (attempt - 1)), 30.0)
    return random.uniform(0, ceiling)


def _should_retry(exc, attempt, max_attempts, method: str = "") -> bool:
    if attempt >= max_attempts:
        return False
    if not isinstance(exc, AchillesError):
        return False
    if method.upper() not in IDEMPOTENT_METHODS:
        return False   # never retry mutating operations
    if exc.code == ERROR_CONNECTION:
        return True
    return exc.status_code in RETRYABLE_STATUSES


def with_retry(
    fn: Callable[[], T],
    max_attempts: int = 3,
) -> T:
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:
            attempt += 1
            if not _should_retry(exc, attempt, max_attempts):
                raise
            retry_after = getattr(exc, "retry_after", None)
            delay = _backoff(attempt, retry_after)
            logger.warning("Attempt %d/%d failed — retrying in %.2fs", attempt, max_attempts, delay)
            time.sleep(delay)


async def with_retry_async(
    fn: Callable[[], T],
    max_attempts: int = 3,
) -> T:
    attempt = 0
    while True:
        try:
            return await fn()
        except Exception as exc:
            attempt += 1
            if not _should_retry(exc, attempt, max_attempts):
                raise
            retry_after = getattr(exc, "retry_after", None)
            delay = _backoff(attempt, retry_after)
            logger.warning("Attempt %d/%d failed — retrying in %.2fs", attempt, max_attempts, delay)
            await asyncio.sleep(delay)
