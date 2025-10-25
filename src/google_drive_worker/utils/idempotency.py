"""Idempotency cache for duplicate detection.

In-memory LRU cache for tracking processed messages and skipping duplicates.
This is a best-effort optimization - the Data Plane enforces final uniqueness.
"""

from collections import OrderedDict
from typing import Optional
import time
import structlog

logger = structlog.get_logger()


class IdempotencyCache:
    """
    In-memory LRU cache for idempotency key tracking.

    Purpose: Skip immediate duplicates during retries (best-effort).
    Note: Not required for correctness - Data Plane enforces uniqueness.

    Features:
    - LRU eviction when max_size is exceeded
    - TTL-based expiration for old entries
    - Thread-safe operations (if needed, can add threading.Lock)
    """

    def __init__(self, max_size: int = 10000, ttl_seconds: int = 86400):
        """Initialize idempotency cache.

        Args:
            max_size: Maximum number of keys to cache (default: 10,000)
            ttl_seconds: Time-to-live for cached keys in seconds (default: 24 hours)
        """
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache: OrderedDict[str, float] = OrderedDict()

        logger.info(
            "Idempotency cache initialized",
            max_size=max_size,
            ttl_seconds=ttl_seconds,
        )

    def contains(self, key: str) -> bool:
        """Check if key exists and is not expired.

        Args:
            key: The idempotency key to check

        Returns:
            True if key exists and is not expired, False otherwise
        """
        if key not in self.cache:
            return False

        timestamp = self.cache[key]
        current_time = time.time()

        # Check if expired
        if current_time - timestamp > self.ttl_seconds:
            del self.cache[key]
            logger.debug(
                "Idempotency key expired",
                key=key,
                age_seconds=current_time - timestamp,
            )
            return False

        # Move to end (LRU) - most recently accessed
        self.cache.move_to_end(key)
        return True

    def add(self, key: str) -> None:
        """Add key to cache.

        Args:
            key: The idempotency key to add
        """
        current_time = time.time()

        # Add or update timestamp
        self.cache[key] = current_time
        self.cache.move_to_end(key)

        # Evict oldest if over capacity
        if len(self.cache) > self.max_size:
            evicted_key, evicted_time = self.cache.popitem(last=False)
            logger.debug(
                "Evicted oldest key from idempotency cache",
                evicted_key=evicted_key,
                age_seconds=current_time - evicted_time,
                cache_size=len(self.cache),
            )

    def size(self) -> int:
        """Get current cache size.

        Returns:
            Number of keys currently in cache
        """
        return len(self.cache)

    def clear(self) -> None:
        """Clear all cached keys."""
        size_before = len(self.cache)
        self.cache.clear()
        logger.info(
            "Idempotency cache cleared",
            keys_removed=size_before,
        )

    def prune_expired(self) -> int:
        """Remove expired keys from cache.

        Returns:
            Number of keys pruned
        """
        current_time = time.time()
        expired_keys = []

        # Find expired keys
        for key, timestamp in self.cache.items():
            if current_time - timestamp > self.ttl_seconds:
                expired_keys.append(key)

        # Remove expired keys
        for key in expired_keys:
            del self.cache[key]

        if expired_keys:
            logger.info(
                "Pruned expired keys from idempotency cache",
                keys_pruned=len(expired_keys),
                remaining_keys=len(self.cache),
            )

        return len(expired_keys)

    def stats(self) -> dict:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        current_time = time.time()
        if self.cache:
            oldest_timestamp = next(iter(self.cache.values()))
            newest_timestamp = next(reversed(self.cache.values()))

            return {
                "size": len(self.cache),
                "max_size": self.max_size,
                "ttl_seconds": self.ttl_seconds,
                "oldest_entry_age_seconds": current_time - oldest_timestamp,
                "newest_entry_age_seconds": current_time - newest_timestamp,
            }
        else:
            return {
                "size": 0,
                "max_size": self.max_size,
                "ttl_seconds": self.ttl_seconds,
                "oldest_entry_age_seconds": None,
                "newest_entry_age_seconds": None,
            }