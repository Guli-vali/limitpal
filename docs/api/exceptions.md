# Exceptions

```python
from limitpal import (
    RateLimitExceeded,    # acquire() timed out
    InvalidConfigError,   # bad constructor args
    CircuitBreakerOpen,   # circuit breaker blocking
    RetryExhausted,       # retries exhausted
)
```

## RateLimitExceeded

Raised when `acquire(key, timeout)` times out.

```python
try:
    limiter.acquire("key", timeout=1.0)
except RateLimitExceeded as e:
    print(e.key, e.retry_after)
```

## InvalidConfigError

Raised for invalid constructor arguments (e.g. negative capacity).

```python
try:
    TokenBucket(capacity=-1, refill_rate=1.0)
except InvalidConfigError as e:
    print(e.parameter, e.value, e.reason)
```

## CircuitBreakerOpen

Raised by `ResilientExecutor` when the circuit breaker is open.

## RetryExhausted

Raised when all retries failed (with `RetryPolicy`).

All of these inherit from `LimitPalError`.
