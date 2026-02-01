"""Composite limiter to combine multiple sync strategies."""

from collections.abc import Iterable

from limitpal.base import SyncLimiter
from limitpal.exceptions import InvalidConfigError, RateLimitExceeded
from limitpal.time import Clock, MonotonicClock


class CompositeLimiter(SyncLimiter):
    """
    Composite rate limiter (sync) that combines multiple limiters.

    An operation is allowed only if all underlying limiters allow it.
    Use different limiters for combined strategies (e.g. TokenBucket + LeakyBucket).

    Example:
        >>> limiter = CompositeLimiter([
        ...     TokenBucket(capacity=10, refill_rate=5),
        ...     LeakyBucket(capacity=20, leak_rate=5),
        ... ])
        >>> if limiter.allow("user:123"):
        ...     process_request()
    """

    def __init__(
        self,
        limiters: Iterable[SyncLimiter],
        clock: Clock | None = None,
    ) -> None:
        """Initialize composite limiter with sync limiters.

        Args:
            limiters: Collection of sync limiters; all must allow for success.
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
    def limiters(self) -> tuple[SyncLimiter, ...]:
        """Tuple of underlying sync limiters."""
        return tuple(self._limiters)

    def allow(self, key: str = "default") -> bool:
        """Check if all limiters allow the operation for the given key.

        Args:
            key: Bucket identifier. Defaults to "default".

        Returns:
            True if all limiters allow, False if any rate limits.
        """
        return all(limiter.allow(key) for limiter in self._limiters)

    def acquire(
        self,
        key: str = "default",
        timeout: float | None = None,
    ) -> None:
        """Block until all limiters allow; acquire in sequence.

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
            limiter.acquire(key, timeout=remaining)
