# Changelog

All notable changes to LimitPal will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-02-01

### Added

- **Token Bucket** (sync and async)
  - Burst allowance with configurable capacity and refill rate
  - `allow()`, `acquire()`, `get_tokens()`, `reset()`
  - TTL and `max_buckets` for automatic bucket eviction

- **Leaky Bucket** (sync and async)
  - Smooth rate limiting with configurable capacity and leak rate
  - `allow()`, `acquire()`, `get_queue_size()`, `get_wait_time()`, `reset()`
  - TTL and `max_buckets` for automatic bucket eviction

- **Composite limiter**
  - `CompositeLimiter` and `AsyncCompositeLimiter` to combine multiple strategies

- **Resilience utilities**
  - `CircuitBreaker` for failure protection
  - `RetryPolicy` with exponential backoff and jitter
  - `ResilientExecutor` and `AsyncResilientExecutor` for combined rate limit + retry + circuit breaker

- **Clock abstraction**
  - `Clock` interface, `MonotonicClock` for production, `MockClock` for deterministic tests

- **Exceptions**
  - `LimitPalError`, `RateLimitExceeded`, `InvalidConfigError`, `CircuitBreakerOpen`, `RetryExhausted`
