"""Base interfaces for sync and async limiters."""

from limitpal.base.async_ import AsyncLimiter
from limitpal.base.sync import SyncLimiter

__all__ = ["AsyncLimiter", "SyncLimiter"]
