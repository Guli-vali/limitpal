"""Leaky Bucket rate limiter implementation (sync).

Requests are queued and processed at a steady leak_rate. Smooth output rate,
no bursts. Supports per-key limiting with optional TTL and LRU eviction.
"""

from __future__ import annotations

import heapq
import threading
import warnings
from dataclasses import dataclass

from limitpal.base import SyncLimiter
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


class LeakyBucket(SyncLimiter):
    """
    Leaky Bucket rate limiter (sync).

    Requests enter a queue of capacity; they leak out at leak_rate per second.
    Produces smooth, steady output with no bursts. Each key has its own bucket.

    Example:
        >>> limiter = LeakyBucket(capacity=10, leak_rate=5)
        >>> if limiter.allow("user:123"):
        ...     process_request()
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
        """Initialize the leaky bucket limiter.

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
        self._buckets_lock = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()
        self._cleanup_lock = threading.Lock()
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

    def _get_lock(self, key: str) -> threading.Lock:
        """Get or create a lock for a specific key; trigger cleanup if needed."""
        with self._locks_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock

        with self._buckets_lock:
            need_room = (
                self._max_buckets is not None
                and key not in self._buckets
                and len(self._buckets) >= self._max_buckets
            )
            self._cleanup_counter += 1
            periodic = (self._ttl is not None or self._max_buckets is not None) and (
                self._cleanup_counter % self._cleanup_interval == 0
            )

        if periodic or need_room:
            now = self._clock.now()
            if need_room and self._max_buckets is not None:
                target_size: int | None = max(0, self._max_buckets - 1)
            else:
                target_size = self._max_buckets
            if self._cleanup_lock.acquire(blocking=False):
                try:
                    self._cleanup(now, target_size=target_size)
                finally:
                    self._cleanup_lock.release()

        return lock

    def _get_or_create_bucket(
        self, key: str, now: float
    ) -> tuple[_LeakyBucketState, bool]:
        """Get existing bucket or create one; return (bucket, created)."""
        with self._buckets_lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _LeakyBucketState(
                    count=0,
                    last_leak=now,
                    leak_accumulator=0.0,
                    last_used=now,
                )
                self._buckets[key] = bucket
                return bucket, True
            return bucket, False

    def _get_existing_lock(self, key: str) -> threading.Lock | None:
        """Return lock for key if it exists; do not create."""
        with self._locks_lock:
            return self._locks.get(key)

    def _evict_bucket(self, key: str, now: float, ttl_check: bool) -> bool:
        """Evict bucket for key; return True if evicted. Skip if in use."""
        lock = self._get_existing_lock(key)
        if lock is None:
            return False
        if not lock.acquire(blocking=False):
            return False
        try:
            with self._locks_lock, self._buckets_lock:
                bucket = self._buckets.get(key)
                if bucket is None:
                    return False
                if (
                    ttl_check
                    and self._ttl is not None
                    and now - bucket.last_used <= self._ttl
                ):
                    return False
                del self._buckets[key]
                self._locks.pop(key, None)
            return True
        finally:
            lock.release()

    def _cleanup(
        self, now: float, target_size: int | None = None
    ) -> None:
        """Evict TTL-expired buckets, then LRU-evict to target_size if set."""
        if self._ttl is None and target_size is None:
            return

        with self._buckets_lock:
            items = list(self._buckets.items())
            current_count = len(items)

        expired_keys: list[str] = []
        if self._ttl is not None:
            for key, bucket in items:
                if now - bucket.last_used > self._ttl:
                    expired_keys.append(key)

        for key in expired_keys:
            if self._evict_bucket(key, now, ttl_check=True):
                current_count -= 1

        if target_size is None or current_count <= target_size:
            return

        expired_set = set(expired_keys)
        eviction_candidates = [
            (bucket.last_used, key)
            for key, bucket in items
            if key not in expired_set
        ]
        to_evict = current_count - target_size
        for _, key in heapq.nsmallest(to_evict, eviction_candidates):
            if self._evict_bucket(key, now, ttl_check=False):
                current_count -= 1
            if current_count <= target_size:
                break

    def _mark_used(self, bucket: _LeakyBucketState) -> None:
        """Update bucket last_used timestamp for TTL/LRU."""
        bucket.last_used = self._clock.now()

    def _leak(self, bucket: _LeakyBucketState, now: float) -> None:
        """Apply leak to bucket based on elapsed time since last_leak."""
        elapsed = now - bucket.last_leak

        if elapsed <= 0:
            return

        if elapsed > 3600:
            bucket.count = 0
            bucket.leak_accumulator = 0.0
            bucket.last_leak = now
            return

        if elapsed > 0 and bucket.count > 0:
            total_leaked = elapsed * self._leak_rate + bucket.leak_accumulator
            leaked_count = int(total_leaked)
            if leaked_count > 0:
                bucket.count = max(0, bucket.count - leaked_count)
                bucket.leak_accumulator = total_leaked - leaked_count
                bucket.last_leak = now
        elif bucket.count == 0:
            bucket.last_leak = now
            bucket.leak_accumulator = 0.0

    def _try_add(self, bucket: _LeakyBucketState, now: float) -> bool:
        """Try to add one request; return True if added, False if full."""
        self._leak(bucket, now)
        if bucket.count < self._capacity:
            bucket.count += 1
            return True
        return False

    def _time_until_space(self, bucket: _LeakyBucketState, now: float) -> float:
        """Seconds until at least one slot is available; 0 if already available."""
        self._leak(bucket, now)
        if bucket.count < self._capacity:
            return 0.0
        requests_to_leak = bucket.count - self._capacity + 1
        effective_requests = max(
            0.0,
            requests_to_leak - bucket.leak_accumulator,
        )
        return max(0.0, effective_requests / self._leak_rate)

    def allow(self, key: str = "default") -> bool:
        """Check if a request can be queued for the given key (non-blocking).

        Args:
            key: Bucket identifier. Defaults to "default".

        Returns:
            True if queued, False if queue is full.
        """
        lock = self._get_lock(key)
        with lock:
            now = self._clock.now()
            bucket, _ = self._get_or_create_bucket(key, now)
            allowed = self._try_add(bucket, now)
            self._mark_used(bucket)
            return allowed

    def acquire(
        self,
        key: str = "default",
        timeout: float | None = None,
    ) -> None:
        """Block until a request can be queued, then queue it.

        Args:
            key: Bucket identifier. Defaults to "default".
            timeout: Maximum seconds to wait. None waits indefinitely.

        Raises:
            RateLimitExceeded: If timeout expires before space is available.
        """
        start_time = self._clock.now()
        while True:
            lock = self._get_lock(key)
            with lock:
                now = self._clock.now()
                bucket, _ = self._get_or_create_bucket(key, now)
                if self._try_add(bucket, now):
                    self._mark_used(bucket)
                    return
                wait_time = self._time_until_space(bucket, now)
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

            self._clock.sleep(wait_time)

    def get_queue_size(self, key: str = "default") -> int:
        """Return current number of requests in queue for the key.

        Args:
            key: Bucket identifier. Defaults to "default".

        Returns:
            Queue size (0 to capacity).
        """
        lock = self._get_lock(key)
        with lock:
            now = self._clock.now()
            bucket, _ = self._get_or_create_bucket(key, now)
            self._leak(bucket, now)
            self._mark_used(bucket)
            return bucket.count

    def get_wait_time(self, key: str = "default") -> float:
        """Return seconds until at least one slot is available.

        Args:
            key: Bucket identifier. Defaults to "default".

        Returns:
            Seconds to wait; 0.0 if space available now.
        """
        lock = self._get_lock(key)
        with lock:
            now = self._clock.now()
            bucket, _ = self._get_or_create_bucket(key, now)
            result = self._time_until_space(bucket, now)
            self._mark_used(bucket)
            return result

    def reset(self, key: str | None = None) -> None:
        """Remove bucket(s), resetting rate limit state.

        Args:
            key: Bucket to reset. If None, clears all buckets.
        """
        if key is None:
            with self._locks_lock:
                locks = list(self._locks.values())
                for lock in locks:
                    lock.acquire()
                try:
                    with self._buckets_lock:
                        self._buckets.clear()
                    self._locks.clear()
                finally:
                    for lock in locks:
                        lock.release()
            return

        lock = self._get_lock(key)
        with lock, self._buckets_lock, self._locks_lock:
            self._buckets.pop(key, None)
            self._locks.pop(key, None)
