# Resilience

## ResilientExecutor / AsyncResilientExecutor

Run callables with optional rate limiting, retry, and circuit breaker. All components are optional.

```python
from limitpal import (
    TokenBucket,
    ResilientExecutor,
    RetryPolicy,
    CircuitBreaker,
)

limiter = TokenBucket(capacity=5, refill_rate=10)
retry = RetryPolicy(max_attempts=3, base_delay=0.2, backoff=2.0)
breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)

executor = ResilientExecutor(
    limiter=limiter,
    retry_policy=retry,
    circuit_breaker=breaker,
)

def call_api() -> str:
    return requests.get("https://api.example.com").text

result = executor.run("api", call_api)
```

## RetryPolicy

```python
RetryPolicy(
    max_attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 5.0,
    backoff: float = 2.0,
    jitter: float = 0.0,
    retry_on: Iterable[type[Exception]] = (Exception,),
)
```

## CircuitBreaker

```python
CircuitBreaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 5.0,
    half_open_success_threshold: int = 1,
    clock: Clock | None = None,
)
```

See [Exceptions](api/exceptions.md) for `CircuitBreakerOpen`, `RetryExhausted`, etc.
