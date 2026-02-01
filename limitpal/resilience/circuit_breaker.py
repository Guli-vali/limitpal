"""Circuit breaker implementation.

Prevents cascading failures by blocking requests when a resource
has exceeded the failure threshold. Supports half-open state for recovery.
"""

import threading

from limitpal.exceptions import InvalidConfigError
from limitpal.time import Clock, MonotonicClock


class CircuitBreaker:
    """
    Circuit breaker for protecting unstable resources.

    States: closed (normal), open (blocking), half_open (testing recovery).
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 5.0,
        half_open_success_threshold: int = 1,
        clock: Clock | None = None,
    ) -> None:
        """Initialize the circuit breaker.

        Args:
            failure_threshold: Failures before opening the circuit.
            recovery_timeout: Seconds before attempting half-open.
            half_open_success_threshold: Successes to close from half-open.
            clock: Time source; defaults to monotonic clock.

        Raises:
            InvalidConfigError: If any threshold or timeout is invalid.
        """
        if failure_threshold <= 0:
            raise InvalidConfigError(
                "failure_threshold must be positive",
                parameter="failure_threshold",
                value=failure_threshold,
                reason="failure_threshold must be > 0",
            )
        if recovery_timeout <= 0:
            raise InvalidConfigError(
                "recovery_timeout must be positive",
                parameter="recovery_timeout",
                value=recovery_timeout,
                reason="recovery_timeout must be > 0",
            )
        if half_open_success_threshold <= 0:
            raise InvalidConfigError(
                "half_open_success_threshold must be positive",
                parameter="half_open_success_threshold",
                value=half_open_success_threshold,
                reason="half_open_success_threshold must be > 0",
            )

        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_success_threshold = half_open_success_threshold
        self._clock = clock or MonotonicClock()
        self._lock = threading.Lock()

        self._state = "closed"
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> str:
        """Current state: 'closed', 'open', or 'half_open'."""
        with self._lock:
            return self._state

    def allow(self) -> bool:
        """Return True if execution is allowed, False if circuit is open."""
        with self._lock:
            if self._state == "open":
                if self._opened_at is None:
                    return False
                if (self._clock.now() - self._opened_at) >= self._recovery_timeout:
                    self._state = "half_open"
                    self._success_count = 0
                    return True
                return False
            return True

    def record_success(self) -> None:
        """Record a successful execution. May transition from half_open to closed."""
        with self._lock:
            if self._state == "half_open":
                self._success_count += 1
                if self._success_count >= self._half_open_success_threshold:
                    self._state = "closed"
                    self._failure_count = 0
            elif self._state == "closed":
                self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed execution. May open the circuit."""
        with self._lock:
            if self._state == "half_open":
                self._trip()
                return
            if self._state == "closed":
                self._failure_count += 1
                if self._failure_count >= self._failure_threshold:
                    self._trip()

    def reset(self) -> None:
        """Reset to closed state and clear all counters."""
        with self._lock:
            self._state = "closed"
            self._failure_count = 0
            self._success_count = 0
            self._opened_at = None

    def _trip(self) -> None:
        """Transition to open state. Caller must hold _lock."""
        assert self._lock.locked(), "_trip must be called with _lock held"
        self._state = "open"
        self._failure_count = 0
        self._success_count = 0
        self._opened_at = self._clock.now()
