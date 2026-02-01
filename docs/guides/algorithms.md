# Algorithms

## Token Bucket

- Tokens refill at `refill_rate` per second
- Each request consumes one token
- Allows bursts up to `capacity`
- Good for APIs that tolerate occasional spikes

## Leaky Bucket

- Requests queue up; they "leak" at `leak_rate` per second
- New requests rejected when queue is full
- Smooth, constant output rate
- Good for steady throughput (background jobs, pipelines)

## Choosing

| Use case | Algorithm |
|----------|-----------|
| API with burst allowance | Token Bucket |
| Smooth rate enforcement | Leaky Bucket |
| Third-party API compliance | Either |

See [API Reference — Limiters](api/limiters.md) for constructor parameters and methods.
