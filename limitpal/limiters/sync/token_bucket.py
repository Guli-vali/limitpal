"""Token Bucket rate limiter implementation (sync).

Tokens are consumed per request; bucket refills at refill_rate. Allows bursts
up to capacity. Supports per-key limiting with optional TTL and LRU eviction.
"""

from __future__ import annotations

import heapq
import threading
from dataclasses import dataclass

from limitpal.base import SyncLimiter
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


class TokenBucket(SyncLimiter):
    """Token Bucket rate limiter (synchronous).

    Allows bursts up to ``capacity`` tokens while sustaining a steady rate of
    ``refill_rate`` tokens per second. Each key has its own bucket. Use
    different keys for per-user, per-IP, or global limits.

    Example:
        >>> limiter = TokenBucket(capacity=10, refill_rate=5)
        >>> if limiter.allow("user:123"):
        ...     process_request()
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
        """Initialize the sync token bucket limiter.

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
            _locks: Per-key lock for concurrent access to each bucket.
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
        self._clock = clock or MonotonicClock()
        self._ttl = ttl
        self._max_buckets = max_buckets
        self._cleanup_interval = cleanup_interval
        self._buckets: dict[str, _BucketState] = {}
        self._buckets_lock = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()
        self._cleanup_lock = threading.Lock()
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

    def _get_lock(
        self,
        key: str,
    ) -> threading.Lock:
        """Get or create per-key lock; trigger cleanup if needed.

        Args:
            key: Bucket identifier.

        Returns:
            Per-key lock for the given key.
        """
        # Per-key locks reduce contention between unrelated keys.
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
        self,
        key: str,
        now: float,
    ) -> tuple[_BucketState, bool]:
        """Get existing bucket or create one; return (bucket, created).

        Args:
            key: Bucket identifier.
            now: Current time (seconds, monotonic).

        Returns:
            Tuple of (bucket state, True if newly created else False).
        """
        with self._buckets_lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _BucketState(
                    tokens=self._capacity,
                    last_refill=now,
                    last_used=now,
                )
                self._buckets[key] = bucket
                return bucket, True
            return bucket, False

    def _refill(
        self,
        bucket: _BucketState,
        now: float,
    ) -> None:
        """Add tokens based on elapsed time since last refill.

        Args:
            bucket: Bucket state to refill.
            now: Current time (seconds, monotonic).
        """
        elapsed = now - bucket.last_refill
        if elapsed > 0:
            new_tokens = elapsed * self._refill_rate
            bucket.tokens = min(self._capacity, bucket.tokens + new_tokens)
            bucket.last_refill = now

    def _try_consume(
        self,
        bucket: _BucketState,
        now: float,
        tokens: int = 1,
    ) -> bool:
        """Consume tokens if available; return True if consumed.

        Args:
            bucket: Bucket state.
            now: Current time (seconds, monotonic).
            tokens: Number of tokens to consume. Defaults to 1.

        Returns:
            True if tokens were consumed, False if insufficient.
        """
        self._refill(bucket, now)
        if bucket.tokens >= tokens:
            bucket.tokens -= tokens
            return True
        return False

    def _time_until_tokens(
        self,
        bucket: _BucketState,
        now: float,
        tokens: int = 1,
    ) -> float:
        """Seconds until at least `tokens` are available (0 if already enough).

        Args:
            bucket: Bucket state.
            now: Current time (seconds, monotonic).
            tokens: Number of tokens needed. Defaults to 1.

        Returns:
            Seconds to wait; 0.0 if tokens already available.
        """
        self._refill(bucket, now)
        if bucket.tokens >= tokens:
            return 0.0
        needed = tokens - bucket.tokens
        return needed / self._refill_rate

    def _cleanup(
        self,
        now: float,
        target_size: int | None = None,
    ) -> None:
        """Evict TTL-expired buckets, then LRU-evict to target_size if set.

        Args:
            now: Current time (seconds, monotonic).
            target_size: Max buckets to keep after LRU eviction; None for TTL-only.
        """
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

        # First evict TTL-expired buckets.
        for key in expired_keys:
            if self._evict_bucket(key, now, ttl_check=True):
                current_count -= 1

        if target_size is None or current_count <= target_size:
            return

        # Then evict least-recently-used buckets by last_used.
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

    def _get_existing_lock(self, key: str) -> threading.Lock | None:
        """Return lock for key if it exists; do not create.

        Args:
            key: Bucket identifier.

        Returns:
            Lock if key exists in _locks, else None.
        """
        with self._locks_lock:
            return self._locks.get(key)

    def _evict_bucket(self, key: str, now: float, ttl_check: bool) -> bool:
        """Evict bucket for key; return True if evicted (uses try-acquire).

        Args:
            key: Bucket identifier to evict.
            now: Current time (seconds, monotonic).
            ttl_check: If True, only evict when bucket exceeded TTL.

        Returns:
            True if bucket was evicted, False if skip (in use or already gone).
        """
        lock = self._get_existing_lock(key)
        if lock is None:
            return False
        # Do not evict buckets currently in use.
        if not lock.acquire(blocking=False):
            return False
        try:
            with self._locks_lock, self._buckets_lock:
                bucket = self._buckets.get(key)
                if bucket is None:
                    return False
                if ttl_check and self._ttl is not None and now - bucket.last_used <= self._ttl:
                    return False
                del self._buckets[key]
                self._locks.pop(key, None)
            return True
        finally:
            lock.release()

    def allow(self, key: str = "default") -> bool:
        """Check if a token can be consumed for the given key (non-blocking).

        Args:
            key: Bucket identifier (e.g. user ID, IP). Defaults to "default".

        Returns:
            True if one token was consumed, False if rate limited.
        """
        lock = self._get_lock(key)
        with lock:
            now = self._clock.now()
            bucket, _ = self._get_or_create_bucket(key, now)
            allowed = self._try_consume(bucket, now)
            bucket.last_used = now
        return allowed

    def acquire(
        self,
        key: str = "default",
        timeout: float | None = None,
    ) -> None:
        """Block until a token is available, then consume it.

        Args:
            key: Bucket identifier (e.g. user ID, IP). Defaults to "default".
            timeout: Maximum seconds to wait. If None, waits indefinitely.

        Raises:
            RateLimitExceeded: If timeout expires before a token is available.
                The exception includes ``retry_after`` (seconds until next token).
        """
        start_time = self._clock.now()
        while True:
            lock = self._get_lock(key)
            with lock:
                now = self._clock.now()
                bucket, _ = self._get_or_create_bucket(key, now)
                if self._try_consume(bucket, now):
                    bucket.last_used = now
                    return
                wait_time = self._time_until_tokens(bucket, now)
                bucket.last_used = now

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

    def get_tokens(self, key: str = "default") -> float:
        """Return current number of tokens in the bucket (after refill).

        Args:
            key: Bucket identifier (e.g. user ID, IP). Defaults to "default".

        Returns:
            Number of tokens available (0.0 to capacity). Fractional values
            are possible due to refill rate.
        """
        lock = self._get_lock(key)
        with lock:
            now = self._clock.now()
            bucket, _ = self._get_or_create_bucket(key, now)
            self._refill(bucket, now)
            bucket.last_used = now
            tokens = bucket.tokens
        return tokens

    def reset(
        self,
        key: str | None = None,
    ) -> None:
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
        with lock:
            with self._buckets_lock:
                self._buckets.pop(key, None)
            with self._locks_lock:
                self._locks.pop(key, None)
