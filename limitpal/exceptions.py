"""Custom exceptions for LimitPal.

All exceptions inherit from LimitPalError. Use RateLimitExceeded for
rate limit timeouts, InvalidConfigError for bad configuration,
CircuitBreakerOpen when circuit breaker blocks, and RetryExhausted
when retries are exhausted.
"""


class LimitPalError(Exception):
    """Base exception for all LimitPal errors."""


class RateLimitExceeded(LimitPalError):
    """
    Raised when a rate limit is exceeded.

    This exception is raised by acquire() methods when the timeout is
    reached before permission is granted (sync or async).

    Attributes:
        key: The rate limit key that was exceeded.
        retry_after: Suggested time to wait before retrying (in seconds).
    """

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        key: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.key = key
        self.retry_after = retry_after

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.key:
            parts.append(f"key={self.key}")
        if self.retry_after is not None:
            parts.append(f"retry_after={self.retry_after:.2f}s")
        return " ".join(parts)


class InvalidConfigError(LimitPalError):
    """
    Raised when limiter configuration is invalid.

    This exception is raised during limiter initialization if the
    provided parameters are invalid (e.g., negative capacity, zero rate).

    Attributes:
        parameter: The name of the invalid parameter.
        value: The invalid value that was provided.
        reason: Explanation of why the value is invalid.
    """

    def __init__(
        self,
        message: str = "Invalid configuration",
        parameter: str | None = None,
        value: object = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(message)
        self.parameter = parameter
        self.value = value
        self.reason = reason

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.parameter:
            parts.append(f"parameter={self.parameter}")
        if self.value is not None:
            parts.append(f"value={self.value}")
        if self.reason:
            parts.append(f"reason={self.reason}")
        return " ".join(parts)


class CircuitBreakerOpen(LimitPalError):
    """
    Raised when the circuit breaker is open and requests are blocked.

    This exception is raised by resilient execution helpers when the
    circuit breaker disallows execution.
    """


class RetryExhausted(LimitPalError):
    """
    Raised when retry attempts are exhausted.

    Attributes:
        attempts: Number of attempts made.
        last_error: The last exception raised.
    """

    def __init__(
        self,
        message: str = "Retry attempts exhausted",
        attempts: int | None = None,
        last_error: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error
