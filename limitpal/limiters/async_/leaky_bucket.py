"""Leaky Bucket rate limiter implementation (async).

Requests are queued and processed at a steady leak_rate. Smooth output rate,
no bursts. Supports per-key limiting with optional TTL and LRU eviction.
"""

import asyncio
import heapq
import warnings
from dataclasses import dataclass

from limitpal.base import AsyncLimiter
from limitpal.exceptions import InvalidConfigError, RateLimitExceeded
from limitpal.time import Clock, MonotonicClock


@dataclass
class _LeakyBucketState:
    """Internal state for a single leaky bucket (per key).

    Attributes:
        count: Current number of requests in the queue.
        last_leak: Timestamp of last leak update.
        leak_accumulator: Fractional leaks carried over.
        last_used: Last access time for TTL/LRU eviction.
    """

    count: int = 0
    last_leak: float = 0.0
    leak_accumulator: float = 0.0
    last_used: float = 0.0


class AsyncLeakyBucket(AsyncLimiter):
    """
    Leaky Bucket rate limiter (async).

    Requests enter a queue of capacity; they leak out at leak_rate per second.
    Produces smooth, steady output with no bursts. Each key has its own bucket.

    Example:
        >>> limiter = AsyncLeakyBucket(capacity=10, leak_rate=5)
        >>> if await limiter.allow("user:123"):
        ...     await process_request()
    """

    def __init__(
        self,
        capacity: int,
        leak_rate: float,
        clock: Clock | None = None,
        ttl: float | None = None,
        max_buckets: int | None = None,
        cleanup_interval: int = 100,
    ) -> None:
        """Initialize the async leaky bucket limiter.

        Args:
            capacity: Maximum queue size (requests).
            leak_rate: Requests processed per second.
            clock: Time source; defaults to monotonic clock.
            ttl: Seconds of inactivity before bucket eviction; None disables.
            max_buckets: Max keys (LRU eviction); None disables.
            cleanup_interval: Run cleanup every N _get_lock calls.

        Raises:
            InvalidConfigError: If capacity, leak_rate, ttl, or max_buckets
                is invalid (non-positive when required).
        """
        if capacity <= 0:
            raise InvalidConfigError(
                "Capacity must be positive",
                parameter="capacity",
                value=capacity,
                reason="capacity must be > 0",
            )
        if leak_rate <= 0:
            raise InvalidConfigError(
                "Leak rate must be positive",
                parameter="leak_rate",
                value=leak_rate,
                reason="leak_rate must be > 0",
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
        self._leak_rate = leak_rate
        self._clock = clock or MonotonicClock()
        self._ttl = ttl
        self._max_buckets = max_buckets
        self._cleanup_interval = cleanup_interval
        self._buckets: dict[str, _LeakyBucketState] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._locks_lock = asyncio.Lock()
        self._cleanup_counter = 0

        max_delay = capacity / leak_rate
        if max_delay > 60:
            warnings.warn(
                "Leaky bucket may introduce large delays due to low leak_rate",
                RuntimeWarning,
                stacklevel=2,
            )

    @property
    def capacity(self) -> int:
        """Maximum queue size (requests)."""
        return self._capacity

    @property
    def leak_rate(self) -> float:
        """Requests processed per second."""
        return self._leak_rate

    async def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create a lock for a specific key; trigger cleanup if needed."""
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
                self._cleanup(now, target_size=target_size)

            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    def _get_or_create_bucket(self, key: str) -> _LeakyBucketState:
        """Get existing bucket or create one. Caller must hold per-key lock."""
        if key not in self._buckets:
            now = self._clock.now()
            self._buckets[key] = _LeakyBucketState(
                count=0,
                last_leak=now,
                leak_accumulator=0.0,
                last_used=now,
            )
        return self._buckets[key]

    def _cleanup(self, now: float, target_size: int | None = None) -> None:
        """Evict TTL-expired buckets, then LRU-evict to target_size if set."""
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

            for key in expired_keys:
                self._buckets.pop(key, None)
                lock = self._locks.get(key)
                if lock is None or not lock.locked():
                    self._locks.pop(key, None)

        if target_size is None:
            return

        if len(self._buckets) <= target_size:
            return

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

    def _mark_used(self, bucket: _LeakyBucketState) -> None:
        """Update bucket last_used timestamp for TTL/LRU."""
        bucket.last_used = self._clock.now()

    def _leak(self, bucket: _LeakyBucketState) -> None:
        """Apply leak to bucket based on elapsed time since last_leak."""
        now = self._clock.now()
        elapsed = now - bucket.last_leak

        if elapsed <= 0:
            return

        if elapsed > 3600:
            bucket.count = 0
            bucket.leak_accumulator = 0.0
            bucket.last_leak = now
            return

        if bucket.count <= 0:
            bucket.count = 0
            bucket.leak_accumulator = 0.0
            bucket.last_leak = now
            return

        leaked_total = elapsed * self._leak_rate + bucket.leak_accumulator
        leaked_count = int(leaked_total)
        if leaked_count > 0:
            if leaked_count >= bucket.count:
                bucket.count = 0
                bucket.leak_accumulator = 0.0
            else:
                bucket.count -= leaked_count
                bucket.leak_accumulator = leaked_total - leaked_count
            bucket.last_leak = now
        else:
            bucket.leak_accumulator = leaked_total

    def _try_add(self, bucket: _LeakyBucketState) -> bool:
        """Try to add one request; return True if added, False if full."""
        self._leak(bucket)
        if bucket.count < self._capacity:
            bucket.count += 1
            return True
        return False

    def _time_until_space(self, bucket: _LeakyBucketState) -> float:
        """Seconds until at least one slot is available; 0 if already available."""
        self._leak(bucket)
        if bucket.count < self._capacity:
            return 0.0
        requests_to_leak = bucket.count - self._capacity + 1
        effective_requests = max(
            0.0,
            requests_to_leak - bucket.leak_accumulator,
        )
        return max(0.0, effective_requests / self._leak_rate)

    async def allow(self, key: str = "default") -> bool:
        """Check if a request can be queued for the given key (non-blocking).

        Args:
            key: Bucket identifier. Defaults to "default".

        Returns:
            True if queued, False if queue is full.
        """
        async with await self._get_lock(key):
            bucket = self._get_or_create_bucket(key)
            result = self._try_add(bucket)
            self._mark_used(bucket)
            return result

    async def acquire(
        self,
        key: str = "default",
        timeout: float | None = None,
    ) -> None:
        """Wait until a request can be queued, then queue it.

        Args:
            key: Bucket identifier. Defaults to "default".
            timeout: Maximum seconds to wait. None waits indefinitely.

        Raises:
            RateLimitExceeded: If timeout expires before space is available.
        """
        start_time = self._clock.now()
        while True:
            async with await self._get_lock(key):
                bucket = self._get_or_create_bucket(key)
                if self._try_add(bucket):
                    self._mark_used(bucket)
                    return
                wait_time = self._time_until_space(bucket)
                self._mark_used(bucket)

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

    async def get_queue_size(self, key: str = "default") -> int:
        """Return current number of requests in queue for the key.

        Args:
            key: Bucket identifier. Defaults to "default".

        Returns:
            Queue size (0 to capacity).
        """
        async with await self._get_lock(key):
            bucket = self._get_or_create_bucket(key)
            self._leak(bucket)
            self._mark_used(bucket)
            return bucket.count

    async def get_wait_time(self, key: str = "default") -> float:
        """Return seconds until at least one slot is available.

        Args:
            key: Bucket identifier. Defaults to "default".

        Returns:
            Seconds to wait; 0.0 if space available now.
        """
        async with await self._get_lock(key):
            bucket = self._get_or_create_bucket(key)
            result = self._time_until_space(bucket)
            self._mark_used(bucket)
            return result

    async def reset(self, key: str | None = None) -> None:
        """Remove bucket(s), resetting rate limit state.

        Args:
            key: Bucket to reset. If None, clears all buckets.
        """
        if key is None:
            async with self._locks_lock:
                locks = list(self._locks.values())
                for lock in locks:
                    await lock.acquire()
                try:
                    self._buckets.clear()
                    self._locks.clear()
                finally:
                    for lock in locks:
                        lock.release()
            return

        lock = await self._get_lock(key)
        async with lock, self._locks_lock:
            self._buckets.pop(key, None)
            self._locks.pop(key, None)
