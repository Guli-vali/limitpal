"""Async tests for Token Bucket rate limiter."""

import asyncio

import pytest

from limitpal import AsyncTokenBucket, MockClock, RateLimitExceeded


@pytest.mark.asyncio
class TestTokenBucketAllowAsync:
    """Test AsyncTokenBucket allow() method."""

    async def test_allow_with_tokens(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=5, refill_rate=1.0, clock=clock)

        result = await limiter.allow("test")
        assert result is True
        assert await limiter.get_tokens("test") == 4.0

    async def test_allow_depletes_tokens(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=3, refill_rate=1.0, clock=clock)

        assert await limiter.allow("test") is True
        assert await limiter.allow("test") is True
        assert await limiter.allow("test") is True
        assert await limiter.allow("test") is False

    async def test_allow_no_tokens(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=1, refill_rate=1.0, clock=clock)

        assert await limiter.allow("test") is True
        assert await limiter.allow("test") is False

    async def test_allow_refills_over_time(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=1, refill_rate=2.0, clock=clock)

        assert await limiter.allow("test") is True
        assert await limiter.allow("test") is False

        clock.advance(0.5)
        assert await limiter.allow("test") is True

    async def test_allow_different_keys(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=1, refill_rate=1.0, clock=clock)

        assert await limiter.allow("user:1") is True
        assert await limiter.allow("user:1") is False
        assert await limiter.allow("user:2") is True


@pytest.mark.asyncio
class TestTokenBucketAcquireAsync:
    """Test AsyncTokenBucket acquire() method."""

    async def test_acquire_with_tokens(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=5, refill_rate=1.0, clock=clock)

        await limiter.acquire("test")
        assert await limiter.get_tokens("test") == 4.0

    async def test_acquire_waits_for_tokens(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=1, refill_rate=2.0, clock=clock)

        await limiter.acquire("test")

        start = clock.now()
        await limiter.acquire("test")
        elapsed = clock.now() - start

        assert 0.4 <= elapsed <= 0.6

    async def test_acquire_timeout_success(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=1, refill_rate=2.0, clock=clock)

        await limiter.acquire("test")
        await limiter.acquire("test", timeout=1.0)

    async def test_acquire_timeout_exceeded(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=1, refill_rate=0.5, clock=clock)

        await limiter.acquire("test")

        with pytest.raises(RateLimitExceeded) as exc_info:
            await limiter.acquire("test", timeout=0.1)

        assert exc_info.value.key == "test"


@pytest.mark.asyncio
class TestTokenBucketAsyncConcurrency:
    """Test AsyncTokenBucket async concurrency."""

    async def test_concurrent_allow(self) -> None:
        limiter = AsyncTokenBucket(capacity=100, refill_rate=0.0001)

        async def worker() -> list[bool]:
            results = []
            for _ in range(10):
                results.append(await limiter.allow("shared"))
            return results

        tasks = [worker() for _ in range(10)]
        all_results = await asyncio.gather(*tasks)

        flat_results = [r for results in all_results for r in results]
        assert sum(flat_results) == 100
        assert len(flat_results) == 100

    async def test_concurrent_acquire(self) -> None:
        clock = MockClock()
        limiter = AsyncTokenBucket(capacity=2, refill_rate=10.0, clock=clock)

        await limiter.acquire("test")
        await limiter.acquire("test")

        async def timed_acquire() -> float:
            start = clock.now()
            await limiter.acquire("test")
            return clock.now() - start

        time1 = await timed_acquire()
        time2 = await timed_acquire()

        assert 0.05 <= time1 <= 0.15
        assert 0.05 <= time2 <= 0.15


@pytest.mark.asyncio
class TestTokenBucketAsyncEviction:
    """Test AsyncTokenBucket TTL/LRU eviction."""

    async def test_ttl_evicts_inactive_buckets(self) -> None:
        """Buckets unused longer than TTL should be evicted on next operation."""
        clock = MockClock()
        limiter = AsyncTokenBucket(
            capacity=1,
            refill_rate=1.0,
            clock=clock,
            ttl=1.0,
            max_buckets=2,
        )

        await limiter.allow("a")
        clock.advance(2.0)

        await limiter.allow("b")
        clock.advance(0.1)

        await limiter.allow("c")  # Triggers cleanup (need_room); "a" expired by TTL

        assert "a" not in limiter._buckets
        assert "b" in limiter._buckets
        assert "c" in limiter._buckets

    async def test_max_buckets_lru_eviction(self) -> None:
        """LRU eviction should remove least-recently-used when over max_buckets."""
        clock = MockClock()
        limiter = AsyncTokenBucket(
            capacity=1,
            refill_rate=1.0,
            clock=clock,
            max_buckets=2,
        )

        await limiter.allow("a")
        clock.advance(1.0)
        await limiter.allow("b")
        clock.advance(1.0)
        await limiter.allow("a")  # "a" more recent than "b"
        clock.advance(1.0)

        await limiter.allow("c")  # Triggers cleanup; "b" is LRU

        assert "a" in limiter._buckets
        assert "b" not in limiter._buckets
        assert "c" in limiter._buckets

    async def test_ttl_eviction_periodic_cleanup(self) -> None:
        """TTL eviction works via periodic cleanup (no max_buckets)."""
        clock = MockClock()
        limiter = AsyncTokenBucket(
            capacity=1,
            refill_rate=1.0,
            clock=clock,
            ttl=1.0,
            cleanup_interval=1,
        )

        await limiter.allow("a")
        clock.advance(2.0)
        await limiter.allow("b")  # 2nd _get_lock triggers periodic cleanup

        assert "a" not in limiter._buckets
        assert "b" in limiter._buckets

    async def test_ttl_and_lru_eviction_order(self) -> None:
        """TTL-expired buckets evicted first, then LRU if still over limit."""
        clock = MockClock()
        limiter = AsyncTokenBucket(
            capacity=1,
            refill_rate=1.0,
            clock=clock,
            ttl=1.0,
            max_buckets=2,
        )

        await limiter.allow("a")  # last_used=0
        clock.advance(0.5)
        await limiter.allow("b")  # last_used=0.5
        clock.advance(0.9)  # now=1.4: "a" expired (1.4>ttl), "b" within TTL (0.9<1.0)

        await limiter.allow("c")  # need_room: evict TTL-expired "a" first, keep "b"

        assert "a" not in limiter._buckets
        assert "b" in limiter._buckets
        assert "c" in limiter._buckets
