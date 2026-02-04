from __future__ import annotations

import asyncio
from dataclasses import dataclass

from limitpal import (
    AsyncCompositeLimiter,
    AsyncLeakyBucket,
    AsyncResilientExecutor,
    AsyncTokenBucket,
    CircuitBreaker,
    CompositeLimiter,
    LeakyBucket,
    MockClock,
    RateLimitExceeded,
    ResilientExecutor,
    RetryPolicy,
    TokenBucket,
)


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _assert_eq(actual: object, expected: object, message: str) -> None:
    if actual != expected:
        raise AssertionError(f"{message} (actual={actual!r}, expected={expected!r})")


def _assert_between(
    value: float,
    low: float,
    high: float,
    message: str,
    *,
    tol: float = 1e-9,
) -> None:
    if value < (low - tol) or value > (high + tol):
        raise AssertionError(f"{message} (value={value}, range=[{low}, {high}])")


@dataclass
class _FlakyService:
    failures_left: int

    def call(self) -> str:
        if self.failures_left > 0:
            self.failures_left -= 1
            raise RuntimeError("upstream error")
        return "ok"

    async def call_async(self) -> str:
        return self.call()


def simulate_sync_limits() -> None:
    clock = MockClock(start_time=0.0)
    # Use exact fractional timing (0.25s) to avoid float-epsilon loops with MockClock.
    user_limiter = TokenBucket(capacity=4, refill_rate=4, clock=clock)
    global_limiter = LeakyBucket(capacity=10, leak_rate=2, clock=clock)
    limiter = CompositeLimiter([user_limiter, global_limiter], clock=clock)

    key = "user:42"
    allowed = sum(1 for _ in range(5) if limiter.allow(key))
    _assert_eq(allowed, 4, "burst should allow exactly capacity tokens")
    _assert_eq(
        global_limiter.get_queue_size(key),
        4,
        "queue should hold 4 requests",
    )

    clock.advance(1.0)
    _assert_eq(
        global_limiter.get_queue_size(key),
        2,
        "queue should leak 2 per second",
    )

    allowed = sum(1 for _ in range(3) if limiter.allow(key))
    _assert_eq(allowed, 3, "subsequent requests should pass after refill")
    _assert_eq(
        global_limiter.get_queue_size(key),
        5,
        "queue should reflect new requests",
    )

    before = clock.now()
    user_limiter.acquire(key, timeout=1.5)
    after = clock.now()
    _assert_between(
        after - before,
        0.0,
        1.0,
        "acquire should not block with tokens",
    )

    user_limiter.allow(key)
    before = clock.now()
    user_limiter.acquire(key, timeout=2.0)
    after = clock.now()
    _assert_between(
        after - before,
        0.25,
        0.25,
        "acquire should wait for refill",
    )

    try:
        tight_queue = LeakyBucket(capacity=1, leak_rate=0.1, clock=clock)
        tight_queue.allow("queue:test")
        tight_queue.acquire("queue:test", timeout=0.1)
    except RateLimitExceeded:
        pass
    else:
        raise AssertionError("leaky bucket should time out when queue is full")


def simulate_sync_resilience() -> None:
    clock = MockClock(start_time=0.0)
    limiter = TokenBucket(capacity=3, refill_rate=3, clock=clock)
    retry = RetryPolicy(
        max_attempts=3,
        base_delay=0.1,
        backoff=2.0,
        jitter=0.0,
    )
    breaker = CircuitBreaker(
        failure_threshold=5,
        recovery_timeout=1.0,
        clock=clock,
    )
    executor = ResilientExecutor(
        limiter=limiter,
        retry_policy=retry,
        circuit_breaker=breaker,
        clock=clock,
    )

    flaky = _FlakyService(failures_left=2)
    before = clock.now()
    result = executor.run("api", flaky.call)
    after = clock.now()
    _assert_eq(result, "ok", "executor should return successful result")
    _assert_between(
        after - before,
        0.3,
        0.3,
        "retry should apply backoff delays",
    )

    breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout=1.0,
        clock=clock,
    )
    executor = ResilientExecutor(circuit_breaker=breaker, clock=clock)

    def always_fail() -> str:
        raise RuntimeError("boom")

    for _ in range(2):
        try:
            executor.run("api", always_fail)
        except RuntimeError:
            pass

    try:
        executor.run("api", always_fail)
    except Exception as exc:
        _assert(
            "CircuitBreakerOpen" in exc.__class__.__name__,
            "breaker should open",
        )
    else:
        raise AssertionError("breaker should block after threshold")

    clock.advance(1.0)
    try:
        executor.run("api", always_fail)
    except RuntimeError:
        pass


async def simulate_async_limits() -> None:
    clock = MockClock(start_time=0.0)
    limiter = AsyncTokenBucket(capacity=2, refill_rate=2, clock=clock)
    key = "user:async"

    allowed = 0
    for _ in range(3):
        if await limiter.allow(key):
            allowed += 1
    _assert_eq(allowed, 2, "async token bucket should allow up to capacity")

    before = clock.now()
    await limiter.acquire(key, timeout=1.0)
    after = clock.now()
    _assert_between(
        after - before,
        0.5,
        0.5,
        "async acquire should wait for refill",
    )

    composite = AsyncCompositeLimiter(
        [
            AsyncTokenBucket(capacity=2, refill_rate=2, clock=clock),
            AsyncLeakyBucket(capacity=3, leak_rate=1, clock=clock),
        ],
        clock=clock,
    )
    allowed = 0
    for _ in range(4):
        if await composite.allow(key):
            allowed += 1
    _assert_eq(
        allowed,
        2,
        "async composite should enforce the strictest limiter",
    )


async def simulate_async_resilience() -> None:
    clock = MockClock(start_time=0.0)
    retry = RetryPolicy(
        max_attempts=2,
        base_delay=0.2,
        backoff=2.0,
        jitter=0.0,
    )
    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout=1.0,
        clock=clock,
    )
    executor = AsyncResilientExecutor(
        retry_policy=retry,
        circuit_breaker=breaker,
        clock=clock,
    )

    flaky = _FlakyService(failures_left=1)
    before = clock.now()
    result = await executor.run("api", flaky.call_async)
    after = clock.now()
    _assert_eq(result, "ok", "async executor should return successful result")
    _assert_between(
        after - before,
        0.2,
        0.2,
        "async retry should apply backoff delay",
    )

    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=1.0,
        clock=clock,
    )
    executor = AsyncResilientExecutor(
        circuit_breaker=breaker,
        clock=clock,
    )

    async def always_fail() -> str:
        raise RuntimeError("boom")

    try:
        await executor.run("api", always_fail)
    except RuntimeError:
        pass

    try:
        await executor.run("api", always_fail)
    except Exception as exc:
        _assert(
            "CircuitBreakerOpen" in exc.__class__.__name__,
            "async breaker should open",
        )
    else:
        raise AssertionError("async breaker should block after threshold")

    clock.advance(1.0)
    try:
        await executor.run("api", always_fail)
    except RuntimeError:
        pass


async def main() -> None:
    simulate_sync_limits()
    simulate_sync_resilience()
    await simulate_async_limits()
    await simulate_async_resilience()
    print("real-world simulation: all checks passed")


if __name__ == "__main__":
    asyncio.run(main())
