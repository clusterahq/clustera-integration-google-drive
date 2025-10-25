"""Unit tests for idempotency cache integration."""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime
import json

from google_drive_worker.worker import GoogleDriveWorker
from google_drive_worker.config import Settings
from clustera_toolkit.idempotency import IdempotencyCache


@pytest.fixture
def settings():
    """Create test settings."""
    settings = Settings()
    settings.worker.idempotency_cache_size = 100
    settings.worker.idempotency_cache_ttl_seconds = 3600
    settings.worker.s3_payload_threshold_bytes = 256 * 1024
    return settings


@pytest.fixture
def worker(settings):
    """Create a worker instance with mocked dependencies."""
    with patch("google_drive_worker.worker.AIOKafkaConsumer"), \
         patch("google_drive_worker.worker.AIOKafkaProducer"):
        worker = GoogleDriveWorker(settings)
        worker.producer = AsyncMock()
        worker.consumer = AsyncMock()
        return worker


@pytest.fixture
def idempotency_cache():
    """Create an idempotency cache instance."""
    return IdempotencyCache(max_size=10, ttl_seconds=60)


class TestIdempotencyCache:
    """Test idempotency cache behavior."""

    def test_cache_initialization(self, idempotency_cache):
        """Test that cache initializes properly."""
        assert idempotency_cache.max_size == 10
        assert idempotency_cache.ttl_seconds == 60
        assert len(idempotency_cache.cache) == 0

    def test_add_and_has(self, idempotency_cache):
        """Test adding items to cache and checking existence."""
        key = "google-drive:conn_123:file:file_abc"

        # Key should not exist initially
        assert not idempotency_cache.has(key)

        # Add key
        idempotency_cache.add(key)

        # Key should now exist
        assert idempotency_cache.has(key)

    def test_lru_eviction(self, idempotency_cache):
        """Test that oldest items are evicted when cache is full."""
        # Fill cache to capacity
        for i in range(10):
            idempotency_cache.add(f"key_{i}")

        # All keys should be present
        for i in range(10):
            assert idempotency_cache.has(f"key_{i}")

        # Add one more item, should evict oldest
        idempotency_cache.add("key_10")

        # Oldest key should be evicted
        assert not idempotency_cache.has("key_0")
        assert idempotency_cache.has("key_10")

    def test_ttl_expiration(self, idempotency_cache):
        """Test that items expire after TTL."""
        key = "expiring_key"

        # Add key with mocked time
        with patch("time.time", return_value=1000):
            idempotency_cache.add(key)
            assert idempotency_cache.has(key)

        # Check after TTL expired
        with patch("time.time", return_value=1061):  # 61 seconds later
            assert not idempotency_cache.has(key)

    def test_clear_cache(self, idempotency_cache):
        """Test clearing the cache."""
        # Add some keys
        for i in range(5):
            idempotency_cache.add(f"key_{i}")

        assert len(idempotency_cache.cache) == 5

        # Clear cache
        idempotency_cache.clear()

        assert len(idempotency_cache.cache) == 0
        for i in range(5):
            assert not idempotency_cache.has(f"key_{i}")


class TestWorkerIdempotency:
    """Test idempotency in the worker."""

    @pytest.mark.asyncio
    async def test_duplicate_record_skipped(self, worker):
        """Test that duplicate records are skipped."""
        record = {
            "message_id": "msg_123",
            "customer_id": "cust_abc",
            "integration_id": "google-drive",
            "integration_connection_id": "conn_123",
            "idempotency_key": "google-drive:conn_123:file:file_xyz",
            "resource_type": "file",
            "resource_id": "file_xyz",
            "data": {"name": "test.txt"},
        }

        # First call should produce
        await worker._produce_record(record)
        assert worker.producer.send.called
        assert worker.metrics["duplicates_skipped"] == 0

        # Reset mock
        worker.producer.send.reset_mock()

        # Second call with same idempotency key should skip
        await worker._produce_record(record)
        assert not worker.producer.send.called
        assert worker.metrics["duplicates_skipped"] == 1

    @pytest.mark.asyncio
    async def test_different_keys_not_skipped(self, worker):
        """Test that different idempotency keys are not skipped."""
        record1 = {
            "message_id": "msg_123",
            "customer_id": "cust_abc",
            "integration_id": "google-drive",
            "integration_connection_id": "conn_123",
            "idempotency_key": "google-drive:conn_123:file:file_001",
            "resource_type": "file",
            "resource_id": "file_001",
            "data": {"name": "file1.txt"},
        }

        record2 = {
            "message_id": "msg_124",
            "customer_id": "cust_abc",
            "integration_id": "google-drive",
            "integration_connection_id": "conn_123",
            "idempotency_key": "google-drive:conn_123:file:file_002",
            "resource_type": "file",
            "resource_id": "file_002",
            "data": {"name": "file2.txt"},
        }

        # Both records should be produced
        await worker._produce_record(record1)
        assert worker.producer.send.call_count == 1

        await worker._produce_record(record2)
        assert worker.producer.send.call_count == 2
        assert worker.metrics["duplicates_skipped"] == 0

    @pytest.mark.asyncio
    async def test_idempotency_cache_size_limit(self, settings):
        """Test that cache respects size limit."""
        settings.worker.idempotency_cache_size = 3
        worker = GoogleDriveWorker(settings)
        worker.producer = AsyncMock()

        # Add 4 records (more than cache size)
        for i in range(4):
            record = {
                "message_id": f"msg_{i}",
                "customer_id": "cust_abc",
                "integration_id": "google-drive",
                "integration_connection_id": "conn_123",
                "idempotency_key": f"google-drive:conn_123:file:file_{i}",
                "resource_type": "file",
                "resource_id": f"file_{i}",
                "data": {"name": f"file{i}.txt"},
            }
            await worker._produce_record(record)

        # First record should have been evicted
        assert not worker.idempotency_cache.has("google-drive:conn_123:file:file_0")
        # Last 3 should still be in cache
        assert worker.idempotency_cache.has("google-drive:conn_123:file:file_1")
        assert worker.idempotency_cache.has("google-drive:conn_123:file:file_2")
        assert worker.idempotency_cache.has("google-drive:conn_123:file:file_3")

    @pytest.mark.asyncio
    async def test_cache_hit_rate_tracking(self, worker):
        """Test that cache hit rate is tracked in metrics."""
        record = {
            "message_id": "msg_123",
            "customer_id": "cust_abc",
            "integration_id": "google-drive",
            "integration_connection_id": "conn_123",
            "idempotency_key": "google-drive:conn_123:file:file_xyz",
            "resource_type": "file",
            "resource_id": "file_xyz",
            "data": {"name": "test.txt"},
        }

        # First call - cache miss
        await worker._produce_record(record)

        # Second call - cache hit
        await worker._produce_record(record)

        # Third call - cache hit
        await worker._produce_record(record)

        assert worker.metrics["duplicates_skipped"] == 2