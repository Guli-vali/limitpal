"""
LimitPal - Your friendly Python rate limiter.

A collection of fast, modular rate limiters for Python async and sync code.
"""

from limitpal.base import AsyncLimiter, SyncLimiter
from limitpal.composite import AsyncCompositeLimiter, CompositeLimiter
from limitpal.exceptions import (
    CircuitBreakerOpen,
    InvalidConfigError,
    LimitPalError,
    RateLimitExceeded,
    RetryExhausted,
)
from limitpal.limiters import AsyncLeakyBucket, AsyncTokenBucket, LeakyBucket, TokenBucket
from limitpal.resilience import (
    AsyncResilientExecutor,
    CircuitBreaker,
    ResilientExecutor,
    RetryPolicy,
)
from limitpal.time import Clock, MockClock, MonotonicClock

__version__ = "0.1.0"

__all__ = [
    # Base
    "SyncLimiter",
    "AsyncLimiter",
    # Limiters (sync)
    "TokenBucket",
    "LeakyBucket",
    # Limiters (async)
    "AsyncTokenBucket",
    "AsyncLeakyBucket",
    # Composite
    "CompositeLimiter",
    "AsyncCompositeLimiter",
    # Resilience
    "RetryPolicy",
    "CircuitBreaker",
    "ResilientExecutor",
    "AsyncResilientExecutor",
    # Clock
    "Clock",
    "MonotonicClock",
    "MockClock",
    # Exceptions
    "LimitPalError",
    "RateLimitExceeded",
    "InvalidConfigError",
    "CircuitBreakerOpen",
    "RetryExhausted",
]
