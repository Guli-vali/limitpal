# CompositeLimiter / AsyncCompositeLimiter

Combine multiple limiters. An operation is allowed only if **all** limiters allow.

```python
from limitpal import CompositeLimiter, TokenBucket, LeakyBucket

limiter = CompositeLimiter([
    TokenBucket(capacity=10, refill_rate=20),  # burst
    LeakyBucket(capacity=20, leak_rate=5),     # steady rate
])

if limiter.allow("user:123"):
    process_request()
```

## Methods

| Method | Description |
|--------|-------------|
| `allow(key)` | `True` if all limiters allow |
| `acquire(key, timeout)` | Block until all allow |
| `limiters` | Tuple of underlying limiters |

Use `AsyncCompositeLimiter` with async limiters for async `allow`/`acquire`.
