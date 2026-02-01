"""Tests for Token Bucket rate limiter (sync)."""

import pytest

from limitpal import InvalidConfigError, MockClock, RateLimitExceeded, TokenBucket


class TestTokenBucketConfig:
    """Test Token Bucket configuration and validation."""

    def test_valid_config(self) -> None:
        """Valid configuration should create limiter successfully."""
        limiter = TokenBucket(capacity=10, refill_rate=1.0)
        assert limiter.capacity == 10
        assert limiter.refill_rate == 1.0

    def test_invalid_capacity_zero(self) -> None:
        """Zero capacity should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            TokenBucket(capacity=0, refill_rate=1.0)
        assert exc_info.value.parameter == "capacity"

    def test_invalid_capacity_negative(self) -> None:
        """Negative capacity should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            TokenBucket(capacity=-5, refill_rate=1.0)
        assert exc_info.value.parameter == "capacity"

    def test_invalid_refill_rate_zero(self) -> None:
        """Zero refill_rate should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            TokenBucket(capacity=10, refill_rate=0)
        assert exc_info.value.parameter == "refill_rate"

    def test_invalid_refill_rate_negative(self) -> None:
        """Negative refill_rate should raise InvalidConfigError."""
        with pytest.raises(InvalidConfigError) as exc_info:
            TokenBucket(capacity=10, refill_rate=-1.0)
        assert exc_info.value.parameter == "refill_rate"


class TestTokenBucketAllow:
    """Test Token Bucket allow() method."""

    def test_allow_with_tokens(self) -> None:
        """allow() should return True when tokens available."""
        clock = MockClock()
        limiter = TokenBucket(capacity=5, refill_rate=1.0, clock=clock)

        assert limiter.allow("test") is True
        assert limiter.get_tokens("test") == 4.0

    def test_allow_depletes_tokens(self) -> None:
        """allow() should consume tokens until depleted."""
        clock = MockClock()
        limiter = TokenBucket(capacity=3, refill_rate=1.0, clock=clock)

        assert limiter.allow("test") is True  # 2 left
        assert limiter.allow("test") is True  # 1 left
        assert limiter.allow("test") is True  # 0 left
        assert limiter.allow("test") is False  # depleted

    def test_allow_no_tokens(self) -> None:
        """allow() should return False when no tokens available."""
        clock = MockClock()
        limiter = TokenBucket(capacity=1, refill_rate=1.0, clock=clock)

        assert limiter.allow("test") is True  # consume the only token
        assert limiter.allow("test") is False  # no tokens left

    def test_allow_refills_over_time(self) -> None:
        """allow() should succeed after tokens refill."""
        clock = MockClock()
        limiter = TokenBucket(capacity=1, refill_rate=2.0, clock=clock)

        assert limiter.allow("test") is True  # consume token
        assert limiter.allow("test") is False  # no tokens

        clock.advance(0.5)  # 0.5s * 2 tokens/s = 1 token refilled
        assert limiter.allow("test") is True

    def test_allow_partial_refill(self) -> None:
        """Tokens should refill partially based on elapsed time."""
        clock = MockClock()
        limiter = TokenBucket(capacity=10, refill_rate=2.0, clock=clock)

        for _ in range(10):
            limiter.allow("test")

        assert limiter.get_tokens("test") == 0.0

        clock.advance(1.0)  # Should have 2 tokens now
        assert limiter.get_tokens("test") == 2.0

    def test_allow_capacity_cap(self) -> None:
        """Tokens should not exceed capacity."""
        clock = MockClock()
        limiter = TokenBucket(capacity=5, refill_rate=10.0, clock=clock)

        clock.advance(100.0)  # Way more than enough to fill
        assert limiter.get_tokens("test") == 5.0

    def test_allow_different_keys(self) -> None:
        """Different keys should have independent buckets."""
        clock = MockClock()
        limiter = TokenBucket(capacity=1, refill_rate=1.0, clock=clock)

        assert limiter.allow("user:1") is True
        assert limiter.allow("user:1") is False  # depleted for user:1
        assert limiter.allow("user:2") is True  # user:2 has own bucket

    def test_allow_default_key(self) -> None:
        """allow() without key should use 'default'."""
        clock = MockClock()
        limiter = TokenBucket(capacity=1, refill_rate=1.0, clock=clock)

        assert limiter.allow() is True
        assert limiter.allow() is False


class TestTokenBucketAcquire:
    """Test Token Bucket acquire() method."""

    def test_acquire_with_tokens(self) -> None:
        """acquire() should return immediately when tokens available."""
        clock = MockClock()
        limiter = TokenBucket(capacity=5, refill_rate=1.0, clock=clock)

        limiter.acquire("test")
        assert limiter.get_tokens("test") == 4.0

    def test_acquire_waits_for_tokens(self) -> None:
        """acquire() should wait for tokens when depleted."""
        clock = MockClock()
        limiter = TokenBucket(capacity=1, refill_rate=2.0, clock=clock)

        limiter.acquire("test")  # consume token

        start = clock.now()
        limiter.acquire("test")  # should wait for refill
        elapsed = clock.now() - start

        assert 0.4 <= elapsed <= 0.6

    def test_acquire_timeout_success(self) -> None:
        """acquire() should succeed within timeout."""
        clock = MockClock()
        limiter = TokenBucket(capacity=1, refill_rate=2.0, clock=clock)

        limiter.acquire("test")
        limiter.acquire("test", timeout=1.0)

    def test_acquire_timeout_exceeded(self) -> None:
        """acquire() should raise RateLimitExceeded on timeout."""
        clock = MockClock()
        limiter = TokenBucket(capacity=1, refill_rate=0.5, clock=clock)

        limiter.acquire("test")

        with pytest.raises(RateLimitExceeded) as exc_info:
            limiter.acquire("test", timeout=0.1)

        assert exc_info.value.key == "test"
        assert exc_info.value.retry_after is not None


class TestTokenBucketReset:
    """Test Token Bucket reset functionality."""

    def test_reset_specific_key(self) -> None:
        """reset() with key should only reset that bucket."""
        clock = MockClock()
        limiter = TokenBucket(capacity=5, refill_rate=1.0, clock=clock)

        for _ in range(5):
            limiter.allow("user:1")
            limiter.allow("user:2")

        limiter.reset("user:1")

        assert limiter.get_tokens("user:1") == 5.0
        assert limiter.get_tokens("user:2") == 0.0

    def test_reset_all(self) -> None:
        """reset() without key should reset all buckets."""
        clock = MockClock()
        limiter = TokenBucket(capacity=5, refill_rate=1.0, clock=clock)

        limiter.allow("user:1")
        limiter.allow("user:2")

        limiter.reset()

        assert limiter.get_tokens("user:1") == 5.0
        assert limiter.get_tokens("user:2") == 5.0


class TestTokenBucketEviction:
    """Test Token Bucket TTL and LRU cleanup."""

    def test_ttl_evicts_inactive_buckets(self) -> None:
        """Buckets unused longer than TTL should be evicted on next operation."""
        clock = MockClock()
        limiter = TokenBucket(
            capacity=1,
            refill_rate=1.0,
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
        limiter = TokenBucket(
            capacity=1,
            refill_rate=1.0,
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
        limiter = TokenBucket(
            capacity=1,
            refill_rate=1.0,
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
        limiter = TokenBucket(
            capacity=1,
            refill_rate=1.0,
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


class TestTokenBucketThreadSafety:
    """Test Token Bucket thread safety."""

    def test_concurrent_allows(self) -> None:
        """Multiple threads should not cause race conditions."""
        import threading

        limiter = TokenBucket(capacity=100, refill_rate=0.0001)
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
