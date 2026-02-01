"""Tests for CompositeLimiter (sync)."""

import pytest

from limitpal import CompositeLimiter, LeakyBucket, MockClock, RateLimitExceeded, TokenBucket


def test_allow_requires_all_limiters() -> None:
    clock = MockClock()
    burst = TokenBucket(capacity=1, refill_rate=1.0, clock=clock)
    steady = LeakyBucket(capacity=1, leak_rate=1.0, clock=clock)
    limiter = CompositeLimiter([burst, steady], clock=clock)

    assert limiter.allow("key") is True
    assert limiter.allow("key") is False

    clock.advance(1.0)
    assert limiter.allow("key") is True


def test_acquire_respects_timeout() -> None:
    clock = MockClock()
    burst = TokenBucket(capacity=1, refill_rate=0.1, clock=clock)
    steady = LeakyBucket(capacity=1, leak_rate=0.1, clock=clock)
    limiter = CompositeLimiter([burst, steady], clock=clock)

    limiter.acquire("key")

    with pytest.raises(RateLimitExceeded):
        limiter.acquire("key", timeout=0.5)
