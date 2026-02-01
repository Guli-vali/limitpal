# LimitPal

**Your friendly Python rate limiter**

A fast, modular rate limiting library for Python with sync and async support. In-memory, zero dependencies, thread-safe.

---

## Installation

```bash
pip install limitpal
```

Or with uv:

```bash
uv add limitpal
```

---

## Quick Start

### Token Bucket (allows bursts)

```python
from limitpal import TokenBucket

limiter = TokenBucket(capacity=5, refill_rate=10)

if limiter.allow("user:123"):
    process_request()
else:
    return "Rate limited"
```

### Leaky Bucket (smooth rate)

```python
from limitpal import LeakyBucket

limiter = LeakyBucket(capacity=10, leak_rate=5)

if limiter.allow("user:123"):
    process_request()
```

### Blocking mode (acquire)

```python
from limitpal import TokenBucket, RateLimitExceeded

limiter = TokenBucket(capacity=1, refill_rate=10)

try:
    limiter.acquire("user:123", timeout=5.0)
    process_request()
except RateLimitExceeded as e:
    print(f"Retry after {e.retry_after}s")
```

### Async

```python
from limitpal import AsyncTokenBucket

limiter = AsyncTokenBucket(capacity=5, refill_rate=10)

if await limiter.allow("user:456"):
    await process_request()

# Or block until allowed
await limiter.acquire("user:456", timeout=5.0)
```

---

## Algorithms

### Token Bucket

- Tokens refill at `refill_rate` per second
- Each request consumes one token
- Allows bursts up to `capacity`
- Good for APIs that tolerate occasional spikes

### Leaky Bucket

- Requests queue up; they "leak" at `leak_rate` per second
- New requests rejected when queue is full
- Smooth, constant output rate
- Good for steady throughput (background jobs, pipelines)

### Choosing

| Use case              | Algorithm   |
|-----------------------|-------------|
| API with burst allowance | Token Bucket |
| Smooth rate enforcement  | Leaky Bucket |
| Third-party API compliance | Either      |

---

## API Reference

### TokenBucket / AsyncTokenBucket

```python
TokenBucket(
    capacity: int,              # Max tokens (burst size)
    refill_rate: float,         # Tokens per second
    clock: Clock | None = None,
    ttl: float | None = None,   # Evict buckets after N seconds idle
    max_buckets: int | None = None,  # Max keys (LRU eviction)
    cleanup_interval: int = 100,
)
```

| Method | Description |
|--------|-------------|
| `allow(key)` | Non-blocking: returns True if token consumed |
| `acquire(key, timeout)` | Block until token available |
| `get_tokens(key)` | Current token count |
| `reset(key)` | Reset bucket; `key=None` clears all |

### LeakyBucket / AsyncLeakyBucket

```python
LeakyBucket(
    capacity: int,              # Max queue size
    leak_rate: float,           # Requests processed per second
    clock: Clock | None = None,
    ttl: float | None = None,
    max_buckets: int | None = None,
    cleanup_interval: int = 100,
)
```

| Method | Description |
|--------|-------------|
| `allow(key)` | Non-blocking: returns True if queued |
| `acquire(key, timeout)` | Block until space available |
| `get_queue_size(key)` | Current queue size |
| `get_wait_time(key)` | Seconds until space available |
| `reset(key)` | Clear queue; `key=None` clears all |

### CompositeLimiter / AsyncCompositeLimiter

Combine multiple limiters. Operation allowed only if all allow.

```python
from limitpal import CompositeLimiter, TokenBucket, LeakyBucket

limiter = CompositeLimiter([
    TokenBucket(capacity=10, refill_rate=20),  # burst
    LeakyBucket(capacity=20, leak_rate=5),     # steady rate
])

if limiter.allow("user:123"):
    process_request()
```

| Method | Description |
|--------|-------------|
| `allow(key)` | True if all limiters allow |
| `acquire(key, timeout)` | Block until all allow |
| `limiters` | Tuple of underlying limiters |

### Per-user + global

```python
from limitpal import TokenBucket

user_limiter = TokenBucket(capacity=5, refill_rate=5)
global_limiter = TokenBucket(capacity=100, refill_rate=100)

if user_limiter.allow("user:123") and global_limiter.allow("global"):
    process_request()
```

---

## Resilience

### ResilientExecutor / AsyncResilientExecutor

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

### RetryPolicy

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

### CircuitBreaker

```python
CircuitBreaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 5.0,
    half_open_success_threshold: int = 1,
    clock: Clock | None = None,
)
```

---

## Testing with MockClock

For deterministic tests:

```python
from limitpal import TokenBucket, MockClock

def test_refill():
    clock = MockClock(start_time=0.0)
    limiter = TokenBucket(capacity=1, refill_rate=2.0, clock=clock)

    assert limiter.allow("test") is True
    assert limiter.allow("test") is False

    clock.advance(0.5)  # 1 token refilled at 2/sec
    assert limiter.allow("test") is True
```

`MockClock` methods: `now()`, `advance(seconds)`, `set_time(value)`, `sleep()`, `sleep_async()`.

---

## Exceptions

```python
from limitpal import (
    RateLimitExceeded,    # acquire() timed out
    InvalidConfigError,   # bad constructor args
    CircuitBreakerOpen,   # circuit breaker blocking
    RetryExhausted,       # retries exhausted
)

# RateLimitExceeded
try:
    limiter.acquire("key", timeout=1.0)
except RateLimitExceeded as e:
    print(e.key, e.retry_after)

# InvalidConfigError
try:
    TokenBucket(capacity=-1, refill_rate=1.0)
except InvalidConfigError as e:
    print(e.parameter, e.value, e.reason)
```

---

## Web Framework Example (FastAPI)

```python
from fastapi import FastAPI, HTTPException, Request
from limitpal import AsyncTokenBucket

app = FastAPI()
limiter = AsyncTokenBucket(capacity=10, refill_rate=5)

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    key = f"ip:{request.client.host}"
    if not await limiter.allow(key):
        raise HTTPException(status_code=429, detail="Too many requests")
    return await call_next(request)
```

---

## Project Structure

```
limitpal/
├── base/         # SyncLimiter, AsyncLimiter interfaces
├── limiters/     # TokenBucket, LeakyBucket (sync + async)
├── composite/    # CompositeLimiter
├── resilience/   # RetryPolicy, CircuitBreaker, ResilientExecutor
├── time/         # Clock, MonotonicClock, MockClock
└── exceptions    # LimitPalError, RateLimitExceeded, etc.
```

---

## Requirements

- Python >= 3.10
- No external dependencies

---

## License

MIT
