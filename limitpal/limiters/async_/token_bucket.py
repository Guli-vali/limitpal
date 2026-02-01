"""Token Bucket rate limiter implementation (async).

Tokens are consumed per request; bucket refills at refill_rate. Allows bursts
up to capacity. Supports per-key limiting with optional TTL and LRU eviction.
"""

import asyncio
import heapq
from dataclasses import dataclass

from limitpal.base import AsyncLimiter
from limitpal.exceptions import InvalidConfigError, RateLimitExceeded
from limitpal.time import Clock, MonotonicClock


@dataclass
class _BucketState:
    """Internal state for a single token bucket (per key).

    Attributes:
        tokens: Current number of tokens available.
        last_refill: Timestamp of last refill.
        last_used: Last access time for TTL/LRU eviction.
    """

    tokens: float
    last_refill: float
    last_used: float


class AsyncTokenBucket(AsyncLimiter):
    """Token Bucket rate limiter (asynchronous).

    Allows bursts up to ``capacity`` tokens while sustaining a steady rate of
    ``refill_rate`` tokens per second. Each key has its own bucket. Use
    different keys for per-user, per-IP, or global limits.

    Example:
        >>> limiter = AsyncTokenBucket(capacity=10, refill_rate=5)
        >>> if await limiter.allow("user:123"):
        ...     await process_request()
    """

    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        clock: Clock | None = None,
        ttl: float | None = None,
        max_buckets: int | None = None,
        cleanup_interval: int = 100,
    ) -> None:
        """Initialize the async token bucket limiter.

        Args:
            capacity: Maximum number of tokens in the bucket (burst size).
            refill_rate: Tokens added per second (sustained rate).
            clock: Time source for refill and sleep; defaults to monotonic clock.
            ttl: Optional time-to-live in seconds; buckets unused longer than
                this are evicted to limit memory.
            max_buckets: Optional cap on number of buckets (keys); LRU eviction
                when exceeded.
            cleanup_interval: Run cleanup every N _get_lock calls (e.g. 100).

        Raises:
            InvalidConfigError: If capacity, refill_rate, ttl, or max_buckets
                is invalid (e.g. non-positive when required).

        Attributes (internal):
            _capacity: Stored capacity (burst limit).
            _refill_rate: Stored refill rate (tokens/sec).
            _ttl: Stored TTL or None if eviction by age is disabled.
            _max_buckets: Stored max buckets or None if uncapped.
            _clock: Time source used for refill and async sleep.
            _buckets: Map key -> _BucketState (tokens, last_refill, last_used).
            _locks: Per-key asyncio.Lock for concurrent access to each bucket.
            _locks_lock: Lock protecting creation/access to _locks and _buckets.
            _cleanup_counter: Incremented on each _get_lock; used for periodic cleanup.
        """
        if capacity <= 0:
            raise InvalidConfigError(
                "Capacity must be positive",
                parameter="capacity",
                value=capacity,
                reason="capacity must be > 0",
            )
        if refill_rate <= 0:
            raise InvalidConfigError(
                "Refill rate must be positive",
                parameter="refill_rate",
                value=refill_rate,
                reason="refill_rate must be > 0",
            )
        if ttl is not None and ttl <= 0:
            raise InvalidConfigError(
                "TTL must be positive",
                parameter="ttl",
                value=ttl,
                reason="ttl must be > 0",
            )
        if max_buckets is not None and max_buckets <= 0:
            raise InvalidConfigError(
                "Max buckets must be positive",
                parameter="max_buckets",
                value=max_buckets,
                reason="max_buckets must be > 0",
            )

        self._capacity = capacity
        self._refill_rate = refill_rate
        self._ttl = ttl
        self._max_buckets = max_buckets
        self._cleanup_interval = cleanup_interval
        self._clock = clock or MonotonicClock()
        self._buckets: dict[str, _BucketState] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._locks_lock = asyncio.Lock()
        self._cleanup_counter = 0

    @property
    def capacity(self) -> int:
        """Maximum tokens in the bucket (burst size).

        Returns:
            Burst capacity (int).
        """
        return self._capacity

    @property
    def refill_rate(self) -> float:
        """Tokens added per second (sustained rate).

        Returns:
            Refill rate in tokens/sec (float).
        """
        return self._refill_rate

    async def _get_lock(
        self,
        key: str,
    ) -> asyncio.Lock:
        """Get or create per-key lock; trigger cleanup if needed.

        Args:
            key: Bucket identifier.

        Returns:
            Per-key lock for the given key.
        """
        async with self._locks_lock:
            self._cleanup_counter += 1
            now: float | None = None
            if (self._ttl is not None or self._max_buckets is not None) and (
                self._cleanup_counter % self._cleanup_interval == 0
            ):
                now = self._clock.now()
                self._cleanup(now, target_size=self._max_buckets)

            if (
                self._max_buckets is not None
                and key not in self._buckets
                and len(self._buckets) >= self._max_buckets
            ):
                if now is None:
                    now = self._clock.now()
                target_size = max(self._max_buckets - 1, 0)
                # Evict TTL-expired buckets first, then LRU to make room.
                self._cleanup(now, target_size=target_size)

            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    def _get_or_create_bucket(self, key: str) -> _BucketState:
        """Get existing bucket or create one.

        Args:
            key: Bucket identifier.

        Returns:
            Bucket state for the given key.
        """
        if key not in self._buckets:
            now = self._clock.now()
            self._buckets[key] = _BucketState(
                tokens=self._capacity,
                last_refill=now,
                last_used=now,
            )
        return self._buckets[key]

    def _refill(self, bucket: _BucketState) -> None:
        """Add tokens based on elapsed time since last refill.

        Args:
            bucket: Bucket state to refill.
        """
        now = self._clock.now()
        elapsed = now - bucket.last_refill
        if elapsed > 0:
            new_tokens = elapsed * self._refill_rate
            bucket.tokens = min(self._capacity, bucket.tokens + new_tokens)
            bucket.last_refill = now

    def _try_consume(self, bucket: _BucketState, tokens: int = 1) -> bool:
        """Consume tokens if available; return True if consumed.

        Args:
            bucket: Bucket state.
            tokens: Number of tokens to consume. Defaults to 1.

        Returns:
            True if tokens were consumed, False if insufficient.
        """
        self._refill(bucket)
        if bucket.tokens >= tokens:
            bucket.tokens -= tokens
            return True
        return False

    def _time_until_tokens(
        self,
        bucket: _BucketState,
        tokens: int = 1,
    ) -> float:
        """Seconds until at least `tokens` are available (0 if already enough).

        Args:
            bucket: Bucket state.
            tokens: Number of tokens needed. Defaults to 1.

        Returns:
            Seconds to wait; 0.0 if tokens already available.
        """
        self._refill(bucket)
        if bucket.tokens >= tokens:
            return 0.0
        needed = tokens - bucket.tokens
        return needed / self._refill_rate

    def _mark_used(self, bucket: _BucketState) -> None:
        """Update bucket's last_used timestamp for LRU/TTL.

        Args:
            bucket: Bucket state to update.
        """
        bucket.last_used = self._clock.now()

    def _cleanup(self, now: float, target_size: int | None = None) -> None:
        """Evict TTL-expired buckets, then LRU-evict to target_size if set.

        Args:
            now: Current time (seconds, monotonic).
            target_size: Max buckets to keep after LRU eviction; None for TTL-only.
        """
        if self._ttl is None and target_size is None:
            return

        ttl = self._ttl
        if ttl is not None:
            expired_keys: list[str] = []
            for key, bucket in self._buckets.items():
                if now - bucket.last_used > ttl:
                    lock = self._locks.get(key)
                    if lock is not None and lock.locked():
                        continue
                    expired_keys.append(key)

            # First evict TTL-expired buckets.
            for key in expired_keys:
                self._buckets.pop(key, None)
                lock = self._locks.get(key)
                if lock is None or not lock.locked():
                    self._locks.pop(key, None)

        if target_size is None:
            return

        if len(self._buckets) <= target_size:
            return

        # Then evict least-recently-used buckets by last_used.
        eviction_candidates: list[tuple[float, str]] = []
        for key, bucket in self._buckets.items():
            lock = self._locks.get(key)
            if lock is not None and lock.locked():
                continue
            eviction_candidates.append((bucket.last_used, key))

        to_evict = len(self._buckets) - target_size
        for _, key in heapq.nsmallest(to_evict, eviction_candidates):
            self._buckets.pop(key, None)
            lock = self._locks.get(key)
            if lock is None or not lock.locked():
                self._locks.pop(key, None)

    async def allow(self, key: str = "default") -> bool:
        """Check if a token can be consumed for the given key (non-blocking).

        Args:
            key: Bucket identifier (e.g. user ID, IP). Defaults to "default".

        Returns:
            True if one token was consumed, False if rate limited.
        """
        async with await self._get_lock(key):
            bucket = self._get_or_create_bucket(key)
            self._mark_used(bucket)
            return self._try_consume(bucket)

    async def acquire(
        self,
        key: str = "default",
        timeout: float | None = None,
    ) -> None:
        """Wait until a token is available, then consume it.

        Args:
            key: Bucket identifier (e.g. user ID, IP). Defaults to "default".
            timeout: Maximum seconds to wait. If None, waits indefinitely.

        Raises:
            RateLimitExceeded: If timeout expires before a token is available.
                The exception includes ``retry_after`` (seconds until next token).
        """
        start_time = self._clock.now()
        while True:
            async with await self._get_lock(key):
                bucket = self._get_or_create_bucket(key)
                self._mark_used(bucket)
                if self._try_consume(bucket):
                    return
                wait_time = self._time_until_tokens(bucket)

            if timeout is not None:
                elapsed = self._clock.now() - start_time
                remaining = timeout - elapsed
                if remaining <= 0:
                    raise RateLimitExceeded(
                        "Timeout waiting for rate limit",
                        key=key,
                        retry_after=wait_time,
                    )
                wait_time = min(wait_time, remaining)

            await self._clock.sleep_async(wait_time)

    async def get_tokens(self, key: str = "default") -> float:
        """Return current number of tokens in the bucket (after refill).

        Args:
            key: Bucket identifier (e.g. user ID, IP). Defaults to "default".

        Returns:
            Number of tokens available (0.0 to capacity). Fractional values
            are possible due to refill rate.
        """
        async with await self._get_lock(key):
            bucket = self._get_or_create_bucket(key)
            self._mark_used(bucket)
            self._refill(bucket)
            return bucket.tokens

    async def reset(self, key: str | None = None) -> None:
        """Remove bucket(s), resetting rate limit state.

        Args:
            key: Bucket to reset. If None, clears all buckets.
        """
        async with self._locks_lock:
            if key is None:
                self._buckets.clear()
                self._locks.clear()
            elif key in self._buckets:
                del self._buckets[key]
                if key in self._locks:
                    del self._locks[key]
