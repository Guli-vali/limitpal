# Cookbook

Real-world patterns for using LimitPal in production apps.

# FastAPI example

Rate limit by client IP using async middleware:

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

Use a composite limiter for per-IP + global limits, or key by user ID when authenticated.


## Resilient HTTP client (retry + circuit breaker + rate limit)

Use a resilient executor to keep a flaky third-party API under control.

```python
import requests

from limitpal import (
    TokenBucket,
    ResilientExecutor,
    RetryPolicy,
    CircuitBreaker,
)

limiter = TokenBucket(capacity=10, refill_rate=20)  # burst + steady
retry = RetryPolicy(max_attempts=4, base_delay=0.2, backoff=2.0, jitter=0.1)
breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=10.0)

executor = ResilientExecutor(
    limiter=limiter,
    retry_policy=retry,
    circuit_breaker=breaker,
)

def fetch_user(user_id: str) -> dict:
    resp = requests.get(f"https://api.example.com/users/{user_id}", timeout=5)
    resp.raise_for_status()
    return resp.json()

result = executor.run("third_party_api", lambda: fetch_user("123"))
```

## Async API client (rate limit + retries)

```python
import aiohttp

from limitpal import AsyncResilientExecutor, AsyncTokenBucket, RetryPolicy

limiter = AsyncTokenBucket(capacity=5, refill_rate=10)
retry = RetryPolicy(max_attempts=3, base_delay=0.1, backoff=2.0)

executor = AsyncResilientExecutor(limiter=limiter, retry_policy=retry)

async def fetch_json(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=5) as resp:
            resp.raise_for_status()
            return await resp.json()

data = await executor.run("api", lambda: fetch_json("https://api.example.com"))
```

## Database query protection (spike control)

Protect your database from sudden spikes (e.g., a hot endpoint or cache miss storm).

```python
from limitpal import TokenBucket

db_reads = TokenBucket(capacity=50, refill_rate=200)  # short bursts, 200 qps steady

def load_profile(user_id: str) -> dict:
    if not db_reads.allow("db:read"):
        raise RuntimeError("DB rate limit exceeded")
    return query_db(user_id)
```

## Message queue consumer with backpressure

Block consumers when downstream is saturated.

```python
from limitpal import LeakyBucket

limiter = LeakyBucket(capacity=500, leak_rate=100)  # 100 msgs/sec

def handle_message(msg) -> None:
    # Apply backpressure to the consumer loop.
    limiter.acquire("mq", timeout=2.0)
    process(msg)
```

## Background jobs with smooth throughput

Use a leaky bucket to keep job processing stable.

```python
from limitpal import LeakyBucket

limiter = LeakyBucket(capacity=100, leak_rate=20)  # 20 jobs/sec

def handle_job(job) -> None:
    # Block until there is room in the queue.
    limiter.acquire("worker", timeout=5.0)
    process(job)
```

## Multi-tier rate limiting (per-user + per-tenant + global)

Protect user fairness, tenant budgets, and overall system capacity.

```python
from limitpal import CompositeLimiter, TokenBucket

user_limiter = TokenBucket(capacity=5, refill_rate=5)
tenant_limiter = TokenBucket(capacity=200, refill_rate=200)
global_limiter = TokenBucket(capacity=1000, refill_rate=1000)

limiter = CompositeLimiter([user_limiter, tenant_limiter, global_limiter])

if limiter.allow("user:123"):
    process_request()
```

## Avoid memory growth with TTL + max buckets

For multi-tenant or unbounded keys, enable eviction.

```python
from limitpal import TokenBucket

limiter = TokenBucket(
    capacity=5,
    refill_rate=10,
    ttl=300,          # Evict buckets after 5 minutes idle
    max_buckets=10_000,
)
```
