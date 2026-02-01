# Limiters API

## TokenBucket / AsyncTokenBucket

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
| `allow(key)` | Non-blocking: returns `True` if token consumed |
| `acquire(key, timeout)` | Block until token available |
| `get_tokens(key)` | Current token count |
| `reset(key)` | Reset bucket; `key=None` clears all |

Async: use `AsyncTokenBucket`; `allow` and `acquire` are async.

---

## LeakyBucket / AsyncLeakyBucket

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
| `allow(key)` | Non-blocking: returns `True` if queued |
| `acquire(key, timeout)` | Block until space available |
| `get_queue_size(key)` | Current queue size |
| `get_wait_time(key)` | Seconds until space available |
| `reset(key)` | Clear queue; `key=None` clears all |

Async: use `AsyncLeakyBucket`; `allow` and `acquire` are async.

---

## Per-user + global

```python
from limitpal import TokenBucket

user_limiter = TokenBucket(capacity=5, refill_rate=5)
global_limiter = TokenBucket(capacity=100, refill_rate=100)

if user_limiter.allow("user:123") and global_limiter.allow("global"):
    process_request()
```
