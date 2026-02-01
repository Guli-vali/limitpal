"""Retry policy configuration for resilient execution.

Exponential backoff with optional jitter and configurable exception handling.
"""

import random
from collections.abc import Iterable
from dataclasses import dataclass

from limitpal.exceptions import InvalidConfigError


@dataclass
class RetryPolicy:
    """
    Retry policy for transient failures.

    Attributes:
        max_attempts: Maximum number of attempts before giving up.
        base_delay: Initial delay between retries (seconds).
        max_delay: Cap on delay between retries.
        backoff: Multiplier for exponential backoff.
        jitter: Random jitter added to delay (0 to disable).
        retry_on: Exception types to retry; others are re-raised.
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 5.0
    backoff: float = 2.0
    jitter: float = 0.0
    retry_on: Iterable[type[Exception]] = (Exception,)

    def __post_init__(self) -> None:
        """Validate policy parameters."""
        if self.max_attempts <= 0:
            raise InvalidConfigError(
                "max_attempts must be positive",
                parameter="max_attempts",
                value=self.max_attempts,
                reason="max_attempts must be > 0",
            )
        if self.base_delay < 0:
            raise InvalidConfigError(
                "base_delay must be non-negative",
                parameter="base_delay",
                value=self.base_delay,
                reason="base_delay must be >= 0",
            )
        if self.max_delay < self.base_delay:
            raise InvalidConfigError(
                "max_delay must be >= base_delay",
                parameter="max_delay",
                value=self.max_delay,
                reason="max_delay must be >= base_delay",
            )
        if self.backoff < 1.0:
            raise InvalidConfigError(
                "backoff must be >= 1.0",
                parameter="backoff",
                value=self.backoff,
                reason="backoff must be >= 1.0",
            )
        if self.jitter < 0:
            raise InvalidConfigError(
                "jitter must be non-negative",
                parameter="jitter",
                value=self.jitter,
                reason="jitter must be >= 0",
            )

    def should_retry(self, exc: Exception, attempt: int) -> bool:
        """Return True if a retry should be attempted for this exception."""
        if attempt >= self.max_attempts:
            return False
        return isinstance(exc, tuple(self.retry_on))

    def next_delay(self, attempt: int) -> float:
        """Return delay in seconds before the next attempt (1-based)."""
        delay = self.base_delay * (self.backoff ** (attempt - 1))
        delay = min(delay, self.max_delay)
        if self.jitter > 0:
            delay += random.uniform(0, self.jitter)
        return delay
