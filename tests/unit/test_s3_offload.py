"""Unit tests for S3 payload offloading."""

import pytest
import asyncio
import json
import hashlib
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime

from google_drive_worker.handlers.base import BaseIntegrationHandler
from google_drive_worker.config import settings
from clustera_integration_toolkit.storage import S3Client


class ConcreteHandler(BaseIntegrationHandler):
    """Concrete implementation for testing."""

    async def can_handle(self, message):
        return True

    async def process_message(self, message, connection_config):
        yield {}

    def generate_idempotency_key(self, connection_id, resource_type, resource_id):
        return f"{self.integration_id}:{connection_id}:{resource_type}:{resource_id}"


class TestS3PayloadOffloading:
    """Test S3 payload offloading functionality."""

    @pytest.fixture
    def handler(self):
        """Create a concrete handler instance."""
        with patch("google_drive_worker.handlers.base.S3Client") as mock_s3:
            handler = ConcreteHandler(integration_id="test-integration")
            handler.s3_client = AsyncMock()
            return handler

    @pytest.fixture
    def large_data(self):
        """Create data larger than threshold."""
        # Create data > 256KB
        return {
            "large_field": "x" * (300 * 1024),  # 300KB of data
            "metadata": {"size": "large"},
        }

    @pytest.fixture
    def small_data(self):
        """Create data smaller than threshold."""
        return {
            "small_field": "x" * 100,  # 100 bytes
            "metadata": {"size": "small"},
        }

    @pytest.mark.asyncio
    async def test_small_payload_inline(self, handler, small_data):
        """Test that small payloads are kept inline."""
        envelope = await handler.create_ingestion_envelope(
            message_id="msg_123",
            customer_id="cust_abc",
            connection_id="conn_123",
            resource_type="file",
            resource_id="file_123",
            data=small_data,
        )

        # Data should be inline
        assert envelope["data"] == small_data
        assert envelope["s3_url"] is None
        assert "sha256" not in envelope["metadata"]
        assert "payload_size_bytes" not in envelope["metadata"]

        # S3 should not be called
        handler.s3_client.upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_large_payload_offloaded(self, handler, large_data):
        """Test that large payloads are offloaded to S3."""
        handler.s3_client.upload.return_value = "s3://bucket/payloads/key.json"

        envelope = await handler.create_ingestion_envelope(
            message_id="msg_123",
            customer_id="cust_abc",
            connection_id="conn_123",
            resource_type="file",
            resource_id="file_123",
            data=large_data,
        )

        # Data should be offloaded
        assert envelope["data"] is None
        assert envelope["s3_url"] == "s3://bucket/payloads/key.json"
        assert "sha256" in envelope["metadata"]
        assert "payload_size_bytes" in envelope["metadata"]

        # S3 should be called
        handler.s3_client.upload.assert_called_once()

        # Verify S3 call arguments
        call_args = handler.s3_client.upload.call_args
        assert call_args[1]["key"] == "payloads/cust_abc/conn_123/msg_123.json"
        assert call_args[1]["content_type"] == "application/json"

        # Verify uploaded data
        uploaded_data = call_args[1]["data"]
        assert json.loads(uploaded_data) == large_data

    @pytest.mark.asyncio
    async def test_checksum_calculation(self, handler, large_data):
        """Test that SHA256 checksum is calculated correctly."""
        handler.s3_client.upload.return_value = "s3://bucket/payloads/key.json"

        envelope = await handler.create_ingestion_envelope(
            message_id="msg_123",
            customer_id="cust_abc",
            connection_id="conn_123",
            resource_type="file",
            resource_id="file_123",
            data=large_data,
        )

        # Calculate expected checksum
        data_bytes = json.dumps(large_data).encode("utf-8")
        expected_checksum = hashlib.sha256(data_bytes).hexdigest()

        assert envelope["metadata"]["sha256"] == expected_checksum

    @pytest.mark.asyncio
    async def test_s3_metadata_included(self, handler, large_data):
        """Test that S3 metadata is included in upload."""
        handler.s3_client.upload.return_value = "s3://bucket/payloads/key.json"

        await handler.create_ingestion_envelope(
            message_id="msg_123",
            customer_id="cust_abc",
            connection_id="conn_123",
            resource_type="file",
            resource_id="file_123",
            data=large_data,
        )

        # Check S3 metadata
        call_args = handler.s3_client.upload.call_args
        metadata = call_args[1]["metadata"]

        assert metadata["customer_id"] == "cust_abc"
        assert metadata["connection_id"] == "conn_123"
        assert metadata["resource_type"] == "file"
        assert metadata["resource_id"] == "file_123"
        assert "sha256" in metadata

    @pytest.mark.asyncio
    async def test_payload_size_recorded(self, handler, large_data):
        """Test that payload size is recorded in metadata."""
        handler.s3_client.upload.return_value = "s3://bucket/payloads/key.json"

        envelope = await handler.create_ingestion_envelope(
            message_id="msg_123",
            customer_id="cust_abc",
            connection_id="conn_123",
            resource_type="file",
            resource_id="file_123",
            data=large_data,
        )

        # Calculate expected size
        data_bytes = json.dumps(large_data).encode("utf-8")
        expected_size = len(data_bytes)

        assert envelope["metadata"]["payload_size_bytes"] == expected_size

    @pytest.mark.asyncio
    async def test_threshold_boundary(self, handler):
        """Test behavior at exactly the threshold boundary."""
        # Create data exactly at threshold (256KB)
        threshold_data = {"field": "x" * (256 * 1024 - 20)}  # Adjust for JSON overhead

        envelope = await handler.create_ingestion_envelope(
            message_id="msg_123",
            customer_id="cust_abc",
            connection_id="conn_123",
            resource_type="file",
            resource_id="file_123",
            data=threshold_data,
        )

        # Check if it's inline or offloaded based on exact size
        data_size = len(json.dumps(threshold_data).encode("utf-8"))
        if data_size > settings.worker.s3_payload_threshold_bytes:
            assert envelope["data"] is None
            assert envelope["s3_url"] is not None
        else:
            assert envelope["data"] == threshold_data
            assert envelope["s3_url"] is None

    @pytest.mark.asyncio
    async def test_s3_upload_failure(self, handler, large_data):
        """Test handling of S3 upload failures."""
        handler.s3_client.upload.side_effect = Exception("S3 upload failed")

        with pytest.raises(Exception, match="S3 upload failed"):
            await handler.create_ingestion_envelope(
                message_id="msg_123",
                customer_id="cust_abc",
                connection_id="conn_123",
                resource_type="file",
                resource_id="file_123",
                data=large_data,
            )

    @pytest.mark.asyncio
    async def test_envelope_completeness_with_offload(self, handler, large_data):
        """Test that all required envelope fields are present with S3 offload."""
        handler.s3_client.upload.return_value = "s3://bucket/payloads/key.json"

        envelope = await handler.create_ingestion_envelope(
            message_id="msg_123",
            customer_id="cust_abc",
            connection_id="conn_123",
            resource_type="file",
            resource_id="file_123",
            data=large_data,
            metadata={"custom": "value"},
        )

        # Check all required fields
        assert envelope["message_id"] == "msg_123"
        assert envelope["customer_id"] == "cust_abc"
        assert envelope["integration_id"] == "test-integration"
        assert envelope["integration_connection_id"] == "conn_123"
        assert envelope["provider"] == "test-integration"
        assert envelope["resource_type"] == "file"
        assert envelope["resource_id"] == "file_123"
        assert envelope["created_at"] is not None
        assert envelope["idempotency_key"] == "test-integration:conn_123:file:file_123"

        # Metadata should include custom values
        assert envelope["metadata"]["custom"] == "value"
        assert envelope["metadata"]["source_format"] == "test-integration_api"
        assert envelope["metadata"]["transformation_version"] == "1.0.0"

    @pytest.mark.asyncio
    async def test_multiple_offloads(self, handler):
        """Test multiple S3 offloads with different data."""
        handler.s3_client.upload.return_value = "s3://bucket/payloads/key.json"

        large_data1 = {"field": "a" * (300 * 1024)}
        large_data2 = {"field": "b" * (400 * 1024)}

        envelope1 = await handler.create_ingestion_envelope(
            message_id="msg_1",
            customer_id="cust_abc",
            connection_id="conn_123",
            resource_type="file",
            resource_id="file_1",
            data=large_data1,
        )

        envelope2 = await handler.create_ingestion_envelope(
            message_id="msg_2",
            customer_id="cust_abc",
            connection_id="conn_123",
            resource_type="file",
            resource_id="file_2",
            data=large_data2,
        )

        # Both should be offloaded
        assert envelope1["s3_url"] is not None
        assert envelope2["s3_url"] is not None

        # S3 should be called twice
        assert handler.s3_client.upload.call_count == 2

        # Different checksums for different data
        assert envelope1["metadata"]["sha256"] != envelope2["metadata"]["sha256"]