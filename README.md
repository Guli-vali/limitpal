# LimitPal

![PyPI version](https://badge.fury.io/py/limitpal.svg)
![Tests](https://github.com/Guli-vali/limitpal/actions/workflows/ci.yml/badge.svg?branch=master)
![Coverage](https://codecov.io/gh/Guli-vali/limitpal/branch/master/graph/badge.svg)
![Python versions](https://img.shields.io/pypi/pyversions/limitpal.svg)

**Your friendly Python resilient execution toolkit**

A fast, modular resilient execution toolkit for Python with **sync** and **async** support. In-memory, zero dependencies, thread-safe.

## Features

- **Resilience**: combine retry, circuit breaker and rate-limiters in one executor
- **Composite limiters** (combine multiple limiters for burst control)
- **Token Bucket** and **Leaky Bucket** algorithms
- **Sync** and **async** APIs for all the functionality
- **MockClock** for deterministic tests
- No external dependencies, Python ≥ 3.10

## When to use LimitPal

**Good fit**

- Building API clients that need fault tolerance (rate limiting + retry + circuit breaker)
- Integrating with unreliable third-party services
- Microservices communication with backpressure (blocking `acquire`)
- Background job processing with rate control

**Not a fit**

- Simple rate limiting without retry logic → use `limits`
- Distributed rate limiting across servers → use a Redis-backed solution.

**Comparison to other solutions**
| Feature | LimitPal | limits | slowapi | tenacity |
|---------|----------|--------|---------|----------|
| Rate Limiting | ✅ | ✅ | ✅ | ❌ |
| Retry Logic | ✅ | ❌ | ❌ | ✅ |
| Circuit Breaker | ✅ | ❌ | ❌ | ❌ |
| Async Support | ✅ | ✅ | ✅ | ✅ |
| Distributed(at least for now 😊) | ❌ | ✅ | ❌ | ❌ |
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

### ResilientExecutor  (async/sync ready)
Combine Limiting + Retry + CircuitBreaker + BurstControl strategies in one executor

```python
""" Async example """

from limitpal import AsyncResilientExecutor, AsyncTokenBucket, CircuitBreaker, RetryPolicy

# Async rate limiting for burst control.
limiter = AsyncTokenBucket(capacity=5, refill_rate=10)
# Async retries with the same policy.
retry = RetryPolicy(max_attempts=3, base_delay=0.2, backoff=2.0)
# Same breaker semantics in async workflows.
breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)

# Async executor wraps limiter + retry + breaker.
executor = AsyncResilientExecutor(
    limiter=limiter,
    retry_policy=retry,
    circuit_breaker=breaker,
)

# Your real-world async call.
async def call_api() -> str:
    return await request_external_service()

# Run with async protection.
result = await executor.run("user:123", call_api)
```

### allow / acquire (same API in sync/async)

`allow()` is non-blocking: it answers “can I proceed right now?”.  
`acquire()` waits for quota (or until `timeout`) and then proceeds.  
Async versions have the same contract — only `await` differs.

```python
from limitpal import TokenBucket

limiter = TokenBucket(capacity=2, refill_rate=1)

if limiter.allow("user:123"):
    process_request()
else:
    return "Rate limited"

limiter.acquire("user:123", timeout=2.0)  # wait until a token is available
process_request()
```

```python
from limitpal import AsyncTokenBucket

limiter = AsyncTokenBucket(capacity=2, refill_rate=1)

if await limiter.allow("user:123"):
    await process_request()
else:
    return "Rate limited"

await limiter.acquire("user:123", timeout=2.0)
await process_request()
```

### Key-based limiting (per-user, per-IP, per-tenant)

Limiters keep separate buckets per key. Use keys to isolate users, IPs, or
any other dimension you need.

```python
from limitpal import TokenBucket

limiter = TokenBucket(capacity=2, refill_rate=1)

# user:123 has its own bucket
limiter.allow("user:123")
limiter.allow("user:123")  # consumes user:123 quota
limiter.allow("user:123")  # likely False (rate limited)

# user:456 is independent
limiter.allow("user:456")  # allowed, separate bucket
```


### Composite limiters (combine strategies)

Use this when you need **both** burst control *and* a smooth global throughput
limit at the same time. All limiters must allow the request. (Sync/Async)

```python
from limitpal import AsyncCompositeLimiter, AsyncLeakyBucket, AsyncTokenBucket

per_user = AsyncTokenBucket(capacity=10, refill_rate=5)
global_smooth = AsyncLeakyBucket(capacity=50, leak_rate=20)

limiter = AsyncCompositeLimiter([per_user, global_smooth])

if await limiter.allow("user:123"):
    await process_request()
else:
    return "Rate limited"
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


---

## Resilience

### ResilientExecutor / AsyncResilientExecutor

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

---

## Documentation

Full documentation: **[limitpal.readthedocs.io](https://limitpal.readthedocs.io/)** (after connecting the repo to [Read the Docs](https://readthedocs.org/) — sign in with GitHub, **Import a Project**, select the repo, leave defaults; RtD will use `.readthedocs.yaml` and build with `pip install -e ".[docs]"` + `mkdocs build`).

To build and serve the docs locally:

```bash
uv sync --group dev
mkdocs serve
```

Open http://127.0.0.1:8000 . To build static HTML: `mkdocs build` (output in `site/`).

---

## License

MIT
