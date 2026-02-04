# LimitPal

**Your friendly Python resilient execution toolkit**

A fast, modular resilient execution toolkit for Python with **sync** and **async** support. In-memory, zero dependencies, thread-safe.

## Features

- **Resilience**: combine retry, circuit breaker and rate-limiters in one executor
- **Composite limiters** (combine multiple limiters for burst control)
- **Token Bucket** and **Leaky Bucket** algorithms
- **Sync** and **async** APIs for all the functionality
- **MockClock** for deterministic tests
- No external dependencies, Python ≥ 3.10

## Installation

```bash
pip install limitpal
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add limitpal
```

## Quick example

### AsyncResilientExecutor/ResilientExecutor (async/sync ready)
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

## Next steps

- [Quick Start](quickstart.md) — basic usage (sync, async, blocking)
- [Algorithms](guides/algorithms.md) — Token Bucket vs Leaky Bucket
- [Cookbook](cookbook.md) — real-world patterns
- [API Reference](api/limiters.md) — full API
- [Examples](examples/fastapi.md) — FastAPI middleware

## Links

- [GitHub](https://github.com/Guli-vali/limitpal)
- [PyPI](https://pypi.org/project/limitpal/)
- [Issues](https://github.com/Guli-vali/limitpal/issues)
