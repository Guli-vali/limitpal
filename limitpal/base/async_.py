"""Asynchronous limiter interface."""

from abc import ABC, abstractmethod


class AsyncLimiter(ABC):
    """
    Base interface for asynchronous rate limiters.
    """

    @abstractmethod
    async def allow(self, key: str = "default") -> bool:
        """
        Check if an operation is allowed for the given key.

        This is a non-blocking check that returns immediately.

        Args:
            key: Bucket identifier (e.g. user ID, IP). Defaults to "default".

        Returns:
            True if the operation is allowed, False if rate limited.
        """

    @abstractmethod
    async def acquire(
        self,
        key: str = "default",
        timeout: float | None = None,
    ) -> None:
        """
        Wait until an operation is allowed for the given key.

        Args:
            key: Bucket identifier. Defaults to "default".
            timeout: Maximum seconds to wait. If None, waits indefinitely.

        Raises:
            RateLimitExceeded: If timeout expires before operation is allowed.
        """
