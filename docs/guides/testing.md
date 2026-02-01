# Testing with MockClock

For deterministic tests, use `MockClock` so time is under your control.

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

## MockClock methods

| Method | Description |
|--------|-------------|
| `now()` | Current time (seconds) |
| `advance(seconds)` | Move time forward |
| `set_time(value)` | Set time to a specific value |
| `sleep()` | Sync sleep (advances time) |
| `sleep_async()` | Async sleep (advances time) |

Pass the same `MockClock` instance to all limiters in a test so they share the same virtual time.
