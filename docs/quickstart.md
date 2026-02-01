# Quick Start

## Token Bucket (allows bursts)

```python
from limitpal import TokenBucket

limiter = TokenBucket(capacity=5, refill_rate=10)

if limiter.allow("user:123"):
    process_request()
else:
    return "Rate limited"
```

## Leaky Bucket (smooth rate)

```python
from limitpal import LeakyBucket

limiter = LeakyBucket(capacity=10, leak_rate=5)

if limiter.allow("user:123"):
    process_request()
```

## Blocking mode (acquire)

```python
from limitpal import TokenBucket, RateLimitExceeded

limiter = TokenBucket(capacity=1, refill_rate=10)

try:
    limiter.acquire("user:123", timeout=5.0)
    process_request()
except RateLimitExceeded as e:
    print(f"Retry after {e.retry_after}s")
```

## Async

```python
from limitpal import AsyncTokenBucket

limiter = AsyncTokenBucket(capacity=5, refill_rate=10)

if await limiter.allow("user:456"):
    await process_request()

# Or block until allowed
await limiter.acquire("user:456", timeout=5.0)
```

## Next

- [Algorithms](guides/algorithms.md) — when to use Token Bucket vs Leaky Bucket
- [API Reference](api/limiters.md) — parameters and methods
