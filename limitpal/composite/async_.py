"""Composite limiter to combine multiple async strategies."""

from collections.abc import Iterable

from limitpal.base import AsyncLimiter
from limitpal.exceptions import InvalidConfigError, RateLimitExceeded
from limitpal.time import Clock, MonotonicClock


class AsyncCompositeLimiter(AsyncLimiter):
    """
    Composite rate limiter (async) that combines multiple limiters.

    An operation is allowed only if all underlying limiters allow it.
    Use different limiters for combined strategies (e.g. TokenBucket + LeakyBucket).

    Example:
        >>> limiter = AsyncCompositeLimiter([
        ...     AsyncTokenBucket(capacity=10, refill_rate=5),
        ...     AsyncLeakyBucket(capacity=20, leak_rate=5),
        ... ])
        >>> if await limiter.allow("user:123"):
        ...     await process_request()
    """

    def __init__(
        self,
        limiters: Iterable[AsyncLimiter],
        clock: Clock | None = None,
    ) -> None:
        """Initialize composite limiter with async limiters.

        Args:
            limiters: Collection of async limiters; all must allow for success.
            clock: Time source for timeout handling; defaults to monotonic clock.

        Raises:
            InvalidConfigError: If limiters is empty.
        """
        self._limiters = list(limiters)
        if not self._limiters:
            raise InvalidConfigError(
                "At least one limiter is required",
                parameter="limiters",
                value=[],
                reason="limiters must contain at least one limiter",
            )
        self._clock = clock or MonotonicClock()

    @property
    def limiters(self) -> tuple[AsyncLimiter, ...]:
        """Tuple of underlying async limiters."""
        return tuple(self._limiters)

    async def allow(self, key: str = "default") -> bool:
        """Check if all limiters allow the operation for the given key.

        Args:
            key: Bucket identifier. Defaults to "default".

        Returns:
            True if all limiters allow, False if any rate limits.
        """
        for limiter in self._limiters:
            if not await limiter.allow(key):
                return False
        return True

    async def acquire(
        self,
        key: str = "default",
        timeout: float | None = None,
    ) -> None:
        """Wait until all limiters allow; acquire in sequence.

        Args:
            key: Bucket identifier. Defaults to "default".
            timeout: Maximum seconds to wait across all limiters.

        Raises:
            RateLimitExceeded: If timeout expires before all limiters allow.
        """
        start = self._clock.now()
        for limiter in self._limiters:
            remaining = None
            if timeout is not None:
                elapsed = self._clock.now() - start
                remaining = timeout - elapsed
                if remaining <= 0:
                    raise RateLimitExceeded(
                        "Timeout waiting for rate limit",
                        key=key,
                        retry_after=0.0,
                    )
            await limiter.acquire(key, timeout=remaining)
