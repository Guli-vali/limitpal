"""Tests for AsyncCompositeLimiter."""

import pytest

from limitpal import AsyncCompositeLimiter, AsyncLeakyBucket, AsyncTokenBucket, MockClock


@pytest.mark.asyncio
async def test_allow_requires_all_limiters() -> None:
    clock = MockClock()
    burst = AsyncTokenBucket(capacity=1, refill_rate=1.0, clock=clock)
    steady = AsyncLeakyBucket(capacity=1, leak_rate=1.0, clock=clock)
    limiter = AsyncCompositeLimiter([burst, steady], clock=clock)

    assert await limiter.allow("key") is True
    assert await limiter.allow("key") is False
    assert await limiter.allow("key") is False
    assert await limiter.allow("key") is False
    assert await limiter.allow("key") is False
    assert await limiter.allow("key") is False
