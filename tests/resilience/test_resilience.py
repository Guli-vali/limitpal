"""Tests for retry and circuit breaker utilities."""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from limitpal import (
    AsyncResilientExecutor,
    CircuitBreaker,
    MockClock,
    ResilientExecutor,
    RetryExhausted,
    RetryPolicy,
)


def test_retry_policy_retries_until_success() -> None:
    clock = MockClock()
    retry = RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0)
    executor = ResilientExecutor(retry_policy=retry, clock=clock)
    attempts = {"count": 0}

    def flaky() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ValueError("boom")
        return "ok"

    result = executor.run("key", flaky)

    assert result == "ok"
    assert attempts["count"] == 3
    assert clock.now() == 3.0


def test_retry_exhausted_raises() -> None:
    clock = MockClock()
    retry = RetryPolicy(max_attempts=2, base_delay=0.5, jitter=0.0)
    executor = ResilientExecutor(retry_policy=retry, clock=clock)

    def always_fail() -> str:
        raise RuntimeError("fail")

    with pytest.raises(RetryExhausted) as exc_info:
        executor.run("key", always_fail)

    assert exc_info.value.attempts == 2
    assert clock.now() == 0.5


def test_circuit_breaker_opens_and_recovers() -> None:
    clock = MockClock()
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=5.0, clock=clock)

    breaker.record_failure()
    breaker.record_failure()

    assert breaker.allow() is False

    clock.advance(5.0)
    assert breaker.allow() is True
    breaker.record_success()
    assert breaker.state == "closed"


@pytest.mark.asyncio
async def test_async_executor_retries_until_success() -> None:
    clock = MockClock()
    retry = RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0)
    executor = AsyncResilientExecutor(retry_policy=retry, clock=clock)
    attempts = {"count": 0}

    async def flaky_async() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ValueError("boom")
        return "ok"

    result = await executor.run("key", flaky_async)

    assert result == "ok"
    assert attempts["count"] == 3
    assert clock.now() == 3.0


class TestCircuitBreakerThreadSafety:
    """Thread-safety tests for CircuitBreaker."""

    def test_concurrent_record_failure_opens_circuit(self) -> None:
        """Many threads calling record_failure() eventually open the circuit."""
        threshold = 10
        num_threads = 20
        calls_per_thread = 5
        breaker = CircuitBreaker(failure_threshold=threshold, recovery_timeout=60.0)

        def record_failures() -> None:
            for _ in range(calls_per_thread):
                breaker.record_failure()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(record_failures) for _ in range(num_threads)]
            for f in as_completed(futures):
                f.result()

        assert breaker.state == "open"
        assert breaker.allow() is False

    def test_concurrent_allow_record_success_failure_no_race(self) -> None:
        """Concurrent allow(), record_success(), record_failure() do not corrupt state."""
        num_threads = 16
        iterations = 200
        breaker = CircuitBreaker(
            failure_threshold=1000,
            recovery_timeout=1.0,
            half_open_success_threshold=2,
        )
        errors: list[Exception] = []
        barrier = threading.Barrier(num_threads)

        def worker(thread_id: int) -> None:
            try:
                barrier.wait()
                for i in range(iterations):
                    breaker.allow()
                    if i % 3 == 0:
                        breaker.record_failure()
                    elif i % 3 == 1:
                        breaker.record_success()
                    state = breaker.state
                    assert state in ("closed", "open", "half_open"), state
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            for f in as_completed(futures):
                f.result()

        assert not errors, errors
        assert breaker.state in ("closed", "open", "half_open")

    def test_concurrent_reset_and_allow_no_race(self) -> None:
        """Concurrent reset() and allow() do not raise or corrupt state."""
        num_threads = 12
        iterations = 100
        breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=10.0)
        errors: list[Exception] = []
        barrier = threading.Barrier(num_threads)

        def resetter(_: int) -> None:
            try:
                barrier.wait()
                for _ in range(iterations):
                    breaker.reset()
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        def allow_only(_: int) -> None:
            try:
                barrier.wait()
                for _ in range(iterations):
                    breaker.allow()
                    assert breaker.state in ("closed", "open", "half_open")
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(resetter if i % 2 == 0 else allow_only, i)
                for i in range(num_threads)
            ]
            for f in as_completed(futures):
                f.result()

        assert not errors, errors
        assert breaker.state in ("closed", "open", "half_open")
