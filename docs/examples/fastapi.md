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
