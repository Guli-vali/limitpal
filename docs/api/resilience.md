# Resilience

## ResilientExecutor / AsyncResilientExecutor

API reference (all parameters are optional):

```python
ResilientExecutor(
    limiter: SyncLimiter | None = None,
    retry_policy: RetryPolicy | None = None,
    circuit_breaker: CircuitBreaker | None = None,
    clock: Clock | None = None,
)
```

```python
AsyncResilientExecutor(
    limiter: AsyncLimiter | None = None,
    retry_policy: RetryPolicy | None = None,
    circuit_breaker: CircuitBreaker | None = None,
    clock: Clock | None = None,
)
```

| Method | Description |
|--------|-------------|
| `run(key, func, *args, **kwargs)` | Execute with limiter, retry, and circuit breaker |

Async: use `AsyncResilientExecutor`; `run` is async.

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

| Method | Description |
|--------|-------------|
| `should_retry(exc, attempt)` | `True` if exception should be retried |
| `next_delay(attempt)` | Delay in seconds before next attempt |

## CircuitBreaker

```python
CircuitBreaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 5.0,
    half_open_success_threshold: int = 1,
    clock: Clock | None = None,
)
```

| Method | Description |
|--------|-------------|
| `state` | Current state: `closed`, `open`, or `half_open` |
| `allow()` | `True` if execution allowed |
| `record_success()` | Record success; may close from half-open |
| `record_failure()` | Record failure; may open circuit |
| `reset()` | Reset to closed and clear counters |
