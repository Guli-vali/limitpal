"""Asynchronous resilient executor.

Runs async callables with optional rate limiting, retry on failure,
and circuit breaker protection.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from limitpal.base import AsyncLimiter
from limitpal.exceptions import CircuitBreakerOpen, RetryExhausted
from limitpal.resilience.circuit_breaker import CircuitBreaker
from limitpal.resilience.retry import RetryPolicy
from limitpal.time import Clock, MonotonicClock

T = TypeVar("T")


class AsyncResilientExecutor:
    """
    Execute async callables with optional rate limiting, retry, and circuit breaker.

    All components are optional; omit for no rate limit, no retry, or no breaker.
    """

    def __init__(
        self,
        limiter: AsyncLimiter | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        clock: Clock | None = None,
    ) -> None:
        """Initialize the async resilient executor.

        Args:
            limiter: Optional rate limiter; acquire before each execution.
            retry_policy: Optional retry policy for transient failures.
            circuit_breaker: Optional circuit breaker for failure protection.
            clock: Time source for delays; defaults to monotonic clock.
        """
        self._limiter = limiter
        self._retry_policy = retry_policy
        self._circuit_breaker = circuit_breaker
        self._clock = clock or MonotonicClock()

    async def run(
        self,
        key: str,
        func: Callable[..., T],
        *args: object,
        **kwargs: object,
    ) -> T:
        """Execute func with rate limit, retry, and circuit breaker.

        Args:
            key: Rate limit key passed to limiter.acquire().
            func: Callable to execute (sync or async). Receives *args, **kwargs.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Result of func(*args, **kwargs).

        Raises:
            CircuitBreakerOpen: If circuit breaker blocks execution.
            RetryExhausted: If retries are exhausted.
        """
        attempt = 1
        breaker = self._circuit_breaker
        retry_policy = self._retry_policy
        retry_exceptions = tuple(retry_policy.retry_on) if retry_policy else (Exception,)
        while True:
            if breaker and not breaker.allow():
                raise CircuitBreakerOpen("Circuit breaker is open")
            try:
                if self._limiter:
                    await self._limiter.acquire(key)
                result = func(*args, **kwargs)
                if hasattr(result, "__await__"):
                    result = await result
            except retry_exceptions as exc:
                if breaker:
                    breaker.record_failure()
                if not retry_policy or not retry_policy.should_retry(
                    exc,
                    attempt,
                ):
                    if retry_policy:
                        raise RetryExhausted(
                            "Retry attempts exhausted",
                            attempts=attempt,
                            last_error=exc,
                        ) from exc
                    raise
                delay = retry_policy.next_delay(attempt)
                await self._clock.sleep_async(delay)
                attempt += 1
                continue
            if breaker:
                breaker.record_success()
            return result
