"""Load tests for sync and async rate limiters."""

from __future__ import annotations

import asyncio
import threading
import time

from limitpal import AsyncLeakyBucket, AsyncTokenBucket, LeakyBucket, TokenBucket

LOAD_DURATION = 0.4
SYNC_WORKERS = 8
ASYNC_TASKS = 16


def _assert_rate_limited(
    allowed: int,
    rejected: int,
    *,
    capacity: int,
    rate: float,
    duration: float,
) -> None:
    assert rejected > 0
    expected_max = capacity + (rate * duration)
    tolerance = max(2.0, rate * 0.2)
    assert allowed <= expected_max + tolerance


def _run_sync_load(
    limiter: LeakyBucket | TokenBucket,
    *,
    duration: float,
    workers: int,
    key: str = "shared",
) -> tuple[int, int, float]:
    start = time.monotonic()
    end = start + duration
    results: list[tuple[int, int]] = []
    results_lock = threading.Lock()

    def worker() -> None:
        allowed = 0
        rejected = 0
        while time.monotonic() < end:
            if limiter.allow(key):
                allowed += 1
            else:
                rejected += 1
        with results_lock:
            results.append((allowed, rejected))

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    total_allowed = sum(item[0] for item in results)
    total_rejected = sum(item[1] for item in results)
    actual_duration = time.monotonic() - start
    return total_allowed, total_rejected, actual_duration


async def _run_async_load(
    limiter: AsyncLeakyBucket | AsyncTokenBucket,
    *,
    duration: float,
    tasks: int,
    key: str = "shared",
) -> tuple[int, int, float]:
    start = time.monotonic()
    end = start + duration

    async def worker() -> tuple[int, int]:
        allowed = 0
        rejected = 0
        iterations = 0
        while time.monotonic() < end:
            if await limiter.allow(key):
                allowed += 1
            else:
                rejected += 1
            iterations += 1
            if iterations % 200 == 0:
                await asyncio.sleep(0)
        return allowed, rejected

    results = await asyncio.gather(*(worker() for _ in range(tasks)))
    total_allowed = sum(item[0] for item in results)
    total_rejected = sum(item[1] for item in results)
    actual_duration = time.monotonic() - start
    return total_allowed, total_rejected, actual_duration


def test_load_sync_token_bucket() -> None:
    limiter = TokenBucket(capacity=5, refill_rate=25.0)
    allowed, rejected, duration = _run_sync_load(
        limiter,
        duration=LOAD_DURATION,
        workers=SYNC_WORKERS,
    )
    _assert_rate_limited(
        allowed,
        rejected,
        capacity=limiter.capacity,
        rate=limiter.refill_rate,
        duration=duration,
    )


def test_load_sync_leaky_bucket() -> None:
    limiter = LeakyBucket(capacity=5, leak_rate=25.0)
    allowed, rejected, duration = _run_sync_load(
        limiter,
        duration=LOAD_DURATION,
        workers=SYNC_WORKERS,
    )
    _assert_rate_limited(
        allowed,
        rejected,
        capacity=limiter.capacity,
        rate=limiter.leak_rate,
        duration=duration,
    )


async def test_load_async_token_bucket() -> None:
    limiter = AsyncTokenBucket(capacity=5, refill_rate=25.0)
    allowed, rejected, duration = await _run_async_load(
        limiter,
        duration=LOAD_DURATION,
        tasks=ASYNC_TASKS,
    )
    _assert_rate_limited(
        allowed,
        rejected,
        capacity=limiter.capacity,
        rate=limiter.refill_rate,
        duration=duration,
    )


async def test_load_async_leaky_bucket() -> None:
    limiter = AsyncLeakyBucket(capacity=5, leak_rate=25.0)
    allowed, rejected, duration = await _run_async_load(
        limiter,
        duration=LOAD_DURATION,
        tasks=ASYNC_TASKS,
    )
    _assert_rate_limited(
        allowed,
        rejected,
        capacity=limiter.capacity,
        rate=limiter.leak_rate,
        duration=duration,
    )
