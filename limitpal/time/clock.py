"""Clock abstraction for time management and testing."""

import time
from abc import ABC, abstractmethod


class Clock(ABC):
    """
    Abstract clock interface for time operations.

    This abstraction allows for deterministic testing by providing
    a mockable time source.
    """

    @abstractmethod
    def now(self) -> float:
        """
        Get the current time in seconds.

        Returns:
            Current time as a float (seconds since some reference point).
        """

    @abstractmethod
    def sleep(self, seconds: float) -> None:
        """
        Sleep for the specified duration.

        Args:
            seconds: Duration to sleep in seconds.
        """

    @abstractmethod
    async def sleep_async(self, seconds: float) -> None:
        """
        Asynchronous sleep for the specified duration.

        Args:
            seconds: Duration to sleep in seconds.
        """


class MonotonicClock(Clock):
    """
    Real clock implementation using monotonic time.

    Uses time.monotonic() for consistent time measurements that are
    not affected by system clock adjustments.
    """

    def now(self) -> float:
        """Get current monotonic time."""
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        """Sleep for the specified duration."""
        if seconds > 0:
            time.sleep(seconds)

    async def sleep_async(self, seconds: float) -> None:
        """Asynchronous sleep for the specified duration."""
        import asyncio

        if seconds > 0:
            await asyncio.sleep(seconds)


class MockClock(Clock):
    """
    Mock clock for deterministic testing.

    Allows manual control of time for predictable test behavior.
    """

    def __init__(self, start_time: float = 0.0) -> None:
        """
        Initialize mock clock.

        Args:
            start_time: Initial time value.
        """
        self._time = start_time

    def now(self) -> float:
        """Get current mock time."""
        return self._time

    def advance(self, seconds: float) -> None:
        """
        Advance the clock by the specified duration.

        Args:
            seconds: Duration to advance in seconds.
        """
        if seconds < 0:
            raise ValueError("Cannot advance clock by negative amount")
        self._time += seconds

    def set_time(self, time_value: float) -> None:
        """
        Set the clock to a specific time.

        Args:
            time_value: The time value to set.
        """
        self._time = time_value

    def sleep(self, seconds: float) -> None:
        """
        Mock sleep that advances the clock.

        In mock mode, sleep immediately advances time without blocking.
        """
        if seconds > 0:
            self.advance(seconds)

    async def sleep_async(self, seconds: float) -> None:
        """
        Mock async sleep that advances the clock.

        In mock mode, sleep immediately advances time without blocking.
        """
        if seconds > 0:
            self.advance(seconds)
