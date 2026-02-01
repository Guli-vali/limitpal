# LimitPal

**Your friendly Python rate limiter**

A fast, modular rate limiting library for Python with **sync** and **async** support. In-memory, zero dependencies, thread-safe.

## Features

- **Token Bucket** and **Leaky Bucket** algorithms
- **Sync** and **async** APIs
- **Composite limiters** (combine multiple limiters)
- **Resilience**: retry, circuit breaker, rate-limited executor
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

```python
from limitpal import TokenBucket

limiter = TokenBucket(capacity=5, refill_rate=10)

if limiter.allow("user:123"):
    process_request()
else:
    return "Rate limited"
```

## Next steps

- [Quick Start](quickstart.md) — basic usage (sync, async, blocking)
- [Algorithms](guides/algorithms.md) — Token Bucket vs Leaky Bucket
- [API Reference](api/limiters.md) — full API
- [Examples](examples/fastapi.md) — FastAPI middleware

## Links

- [GitHub](https://github.com/Guli-vali/limitpal)
- [PyPI](https://pypi.org/project/limitpal/)
- [Issues](https://github.com/Guli-vali/limitpal/issues)
