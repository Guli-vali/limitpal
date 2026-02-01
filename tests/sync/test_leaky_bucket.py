"""Tests for Leaky Bucket rate limiter (sync)."""

import pytest

from limitpal import InvalidConfigError, LeakyBucket, MockClock, RateLimitExceeded


class TestLeakyBucketConfig:
    """Test Leaky Bucket configuration and validation."""

    def test_valid_config(self) -> None:
        """Valid configuration should create limiter successfully."""
        limiter = LeakyBucket(capacity=10, leak_rate=1.0)
        assert limiter.capacity == 10
        assert limiter.leak_rate == 1.0

    def test_invalid_capacity_zero(self) -> None:
        """Zero capacity should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            LeakyBucket(capacity=0, leak_rate=1.0)
        assert exc_info.value.parameter == "capacity"

    def test_invalid_capacity_negative(self) -> None:
        """Negative capacity should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            LeakyBucket(capacity=-5, leak_rate=1.0)
        assert exc_info.value.parameter == "capacity"

    def test_invalid_leak_rate_zero(self) -> None:
        """Zero leak_rate should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            LeakyBucket(capacity=10, leak_rate=0)
        assert exc_info.value.parameter == "leak_rate"

    def test_invalid_leak_rate_negative(self) -> None:
        """Negative leak_rate should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            LeakyBucket(capacity=10, leak_rate=-1.0)
        assert exc_info.value.parameter == "leak_rate"

    def test_invalid_ttl_zero(self) -> None:
        """Zero ttl should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            LeakyBucket(capacity=10, leak_rate=1.0, ttl=0)
        assert exc_info.value.parameter == "ttl"

    def test_invalid_ttl_negative(self) -> None:
        """Negative ttl should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            LeakyBucket(capacity=10, leak_rate=1.0, ttl=-1.0)
        assert exc_info.value.parameter == "ttl"

    def test_invalid_max_buckets_zero(self) -> None:
        """Zero max_buckets should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            LeakyBucket(capacity=10, leak_rate=1.0, max_buckets=0)
        assert exc_info.value.parameter == "max_buckets"


class TestLeakyBucketAllow:
    """Test Leaky Bucket allow() method."""

    def test_allow_empty_queue(self) -> None:
        """allow() should return True when queue is empty."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=5, leak_rate=1.0, clock=clock)

        assert limiter.allow("test") is True
        assert limiter.get_queue_size("test") == 1

    def test_allow_fills_queue(self) -> None:
        """allow() should fill queue up to capacity."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=3, leak_rate=1.0, clock=clock)

        assert limiter.allow("test") is True
        assert limiter.allow("test") is True
        assert limiter.allow("test") is True
        assert limiter.allow("test") is False

    def test_allow_queue_full(self) -> None:
        """allow() should return False when queue is full."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=1, leak_rate=1.0, clock=clock)

        assert limiter.allow("test") is True
        assert limiter.allow("test") is False

    def test_allow_after_leak(self) -> None:
        """allow() should succeed after queue leaks."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=2, leak_rate=2.0, clock=clock)

        assert limiter.allow("test") is True
        assert limiter.allow("test") is True
        assert limiter.allow("test") is False

        clock.advance(0.5)
        assert limiter.allow("test") is True

    def test_allow_multiple_leaks(self) -> None:
        """Multiple items should leak over time."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=5, leak_rate=2.0, clock=clock)

        for _ in range(5):
            limiter.allow("test")

        assert limiter.get_queue_size("test") == 5

        clock.advance(1.0)
        assert limiter.get_queue_size("test") == 3

        clock.advance(1.5)
        assert limiter.get_queue_size("test") == 0

    def test_allow_different_keys(self) -> None:
        """Different keys should have independent queues."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=1, leak_rate=1.0, clock=clock)

        assert limiter.allow("user:1") is True
        assert limiter.allow("user:1") is False
        assert limiter.allow("user:2") is True

    def test_allow_default_key(self) -> None:
        """allow() without key should use 'default'."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=1, leak_rate=1.0, clock=clock)

        assert limiter.allow() is True
        assert limiter.allow() is False


class TestLeakyBucketAcquire:
    """Test Leaky Bucket acquire() method."""

    def test_acquire_empty_queue(self) -> None:
        """acquire() should return immediately when queue has space."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=5, leak_rate=1.0, clock=clock)

        limiter.acquire("test")
        assert limiter.get_queue_size("test") == 1

    def test_acquire_waits_for_space(self) -> None:
        """acquire() should wait when queue is full."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=1, leak_rate=2.0, clock=clock)

        limiter.acquire("test")

        start = clock.now()
        limiter.acquire("test")
        elapsed = clock.now() - start

        assert 0.4 <= elapsed <= 0.6

    def test_acquire_timeout_success(self) -> None:
        """acquire() should succeed within timeout."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=1, leak_rate=2.0, clock=clock)

        limiter.acquire("test")
        limiter.acquire("test", timeout=1.0)

    def test_acquire_timeout_exceeded(self) -> None:
        """acquire() should raise RateLimitExceeded on timeout."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=1, leak_rate=0.5, clock=clock)

        limiter.acquire("test")

        with pytest.raises(RateLimitExceeded) as exc_info:
            limiter.acquire("test", timeout=0.1)

        assert exc_info.value.key == "test"
        assert exc_info.value.retry_after is not None


class TestLeakyBucketReset:
    """Test Leaky Bucket reset functionality."""

    def test_reset_specific_key(self) -> None:
        """reset() with key should only reset that queue."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=5, leak_rate=1.0, clock=clock)

        for _ in range(3):
            limiter.allow("user:1")
            limiter.allow("user:2")

        limiter.reset("user:1")

        assert limiter.get_queue_size("user:1") == 0
        assert limiter.get_queue_size("user:2") == 3

    def test_reset_all(self) -> None:
        """reset() without key should reset all queues."""
        clock = MockClock()
        limiter = LeakyBucket(capacity=5, leak_rate=1.0, clock=clock)

        limiter.allow("user:1")
        limiter.allow("user:2")

        limiter.reset()

        assert limiter.get_queue_size("user:1") == 0
        assert limiter.get_queue_size("user:2") == 0


class TestLeakyBucketEviction:
    """Test Leaky Bucket TTL and LRU cleanup."""

    def test_ttl_evicts_inactive_buckets(self) -> None:
        """Buckets unused longer than TTL should be evicted on next operation."""
        clock = MockClock()
        limiter = LeakyBucket(
            capacity=5,
            leak_rate=1.0,
            clock=clock,
            ttl=1.0,
            max_buckets=2,
        )

        limiter.allow("a")
        clock.advance(2.0)

        limiter.allow("b")
        clock.advance(0.1)

        limiter.allow("c")  # Triggers cleanup (need_room); "a" expired by TTL

        assert "a" not in limiter._buckets
        assert "b" in limiter._buckets
        assert "c" in limiter._buckets

    def test_max_buckets_lru_eviction(self) -> None:
        """LRU eviction should remove least-recently-used when over max_buckets."""
        clock = MockClock()
        limiter = LeakyBucket(
            capacity=5,
            leak_rate=1.0,
            clock=clock,
            max_buckets=2,
        )

        limiter.allow("a")
        clock.advance(1.0)
        limiter.allow("b")
        clock.advance(1.0)
        limiter.allow("a")  # "a" more recent than "b"
        clock.advance(1.0)

        limiter.allow("c")  # Triggers cleanup; "b" is LRU

        assert "a" in limiter._buckets
        assert "b" not in limiter._buckets
        assert "c" in limiter._buckets

    def test_ttl_eviction_periodic_cleanup(self) -> None:
        """TTL eviction works via periodic cleanup (no max_buckets)."""
        clock = MockClock()
        limiter = LeakyBucket(
            capacity=5,
            leak_rate=1.0,
            clock=clock,
            ttl=1.0,
            cleanup_interval=1,
        )

        limiter.allow("a")
        clock.advance(2.0)
        limiter.allow("b")  # 2nd _get_lock triggers periodic cleanup

        assert "a" not in limiter._buckets
        assert "b" in limiter._buckets

    def test_ttl_and_lru_eviction_order(self) -> None:
        """TTL-expired buckets evicted first, then LRU if still over limit."""
        clock = MockClock()
        limiter = LeakyBucket(
            capacity=5,
            leak_rate=1.0,
            clock=clock,
            ttl=1.0,
            max_buckets=2,
        )

        limiter.allow("a")  # last_used=0
        clock.advance(0.5)
        limiter.allow("b")  # last_used=0.5
        clock.advance(0.9)  # now=1.4: "a" expired (1.4>ttl), "b" within TTL (0.9<1.0)

        limiter.allow("c")  # need_room: evict TTL-expired "a" first, keep "b"

        assert "a" not in limiter._buckets
        assert "b" in limiter._buckets
        assert "c" in limiter._buckets


class TestLeakyBucketThreadSafety:
    """Test Leaky Bucket thread safety."""

    def test_concurrent_allows(self) -> None:
        """Multiple threads should not cause race conditions."""
        import threading

        limiter = LeakyBucket(capacity=100, leak_rate=0.0001)
        successes = []
        lock = threading.Lock()

        def worker() -> None:
            for _ in range(10):
                result = limiter.allow("shared")
                with lock:
                    successes.append(result)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(successes) == 100
        assert len(successes) == 100
