"""Integration tests for end-to-end Kafka flows."""

import asyncio
import json
from typing import Any, Dict
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from testcontainers.kafka import KafkaContainer

from google_drive_worker.worker import GoogleDriveWorker


@pytest.mark.integration
class TestKafkaFlow:
    """Integration tests for Kafka message flows."""

    @pytest.mark.asyncio
    async def test_end_to_end_trigger_flow(
        self,
        kafka_container: KafkaContainer,
        kafka_config: Dict[str, Any],
        kafka_producer: AIOKafkaProducer,
        sample_trigger_message: Dict[str, Any],
        sample_google_file: Dict[str, Any]
    ):
        """Test complete flow from trigger to ingestion.data."""
        # Setup topics
        trigger_topic = "integration.trigger"
        ingestion_topic = "ingestion.data"

        # Create consumer for ingestion topic
        consumer = AIOKafkaConsumer(
            ingestion_topic,
            bootstrap_servers=kafka_config["bootstrap_servers"],
            group_id="test-consumer",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        await consumer.start()

        try:
            # Produce trigger message
            await kafka_producer.send(
                trigger_topic,
                key=sample_trigger_message["integration_connection_id"],
                value=sample_trigger_message
            )
            await kafka_producer.flush()

            # Mock Google API client
            with patch("google_drive_worker.google_client.GoogleDriveClient") as MockClient:
                mock_client = AsyncMock()
                MockClient.return_value = mock_client

                # Mock API responses
                mock_client.list_files.return_value = {
                    "files": [sample_google_file],
                    "nextPageToken": None
                }
                mock_client.get_file_metadata.return_value = sample_google_file
                mock_client.list_permissions.return_value = {
                    "permissions": sample_google_file["permissions"]
                }

                # Create and start worker
                worker = GoogleDriveWorker(
                    kafka_config={
                        "bootstrap_servers": kafka_config["bootstrap_servers"],
                        "consumer_group_id": kafka_config["consumer_group_id"],
                        "auto_offset_reset": "earliest",
                        "enable_auto_commit": False,
                    },
                    worker_config={
                        "integration_id": "google-drive",
                        "max_concurrent_connections": 5,
                        "s3_payload_threshold_bytes": 256 * 1024,
                    }
                )

                # Process one message
                process_task = asyncio.create_task(worker.process_single_message())

                # Wait for processing with timeout
                await asyncio.wait_for(process_task, timeout=10.0)

                # Consume from ingestion topic
                messages = []
                async for msg in consumer:
                    messages.append(msg.value)
                    if len(messages) >= 1:
                        break

                # Verify ingestion message
                assert len(messages) == 1
                ingestion_msg = messages[0]

                # Verify message structure
                assert ingestion_msg["message_id"]
                assert ingestion_msg["customer_id"] == "cust_xyz"
                assert ingestion_msg["integration_id"] == "google-drive"
                assert ingestion_msg["integration_connection_id"] == "conn_abc123"
                assert ingestion_msg["data_type"] == "file"
                assert ingestion_msg["idempotency_key"]

                # Verify file data
                file_data = ingestion_msg["data"]
                assert file_data["id"] == "file_123"
                assert file_data["name"] == "test_document.pdf"
                assert file_data["mimeType"] == "application/pdf"

        finally:
            await consumer.stop()

    @pytest.mark.asyncio
    async def test_webhook_flow(
        self,
        kafka_container: KafkaContainer,
        kafka_config: Dict[str, Any],
        kafka_producer: AIOKafkaProducer,
        sample_webhook_message: Dict[str, Any],
        sample_google_file: Dict[str, Any]
    ):
        """Test webhook notification processing."""
        webhook_topic = "webhook.raw"
        ingestion_topic = "ingestion.data"

        # Create consumer for ingestion topic
        consumer = AIOKafkaConsumer(
            ingestion_topic,
            bootstrap_servers=kafka_config["bootstrap_servers"],
            group_id="test-consumer-webhook",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await consumer.start()

        try:
            # Produce webhook message
            await kafka_producer.send(
                webhook_topic,
                key=sample_webhook_message["integration_connection_id"],
                value=sample_webhook_message
            )
            await kafka_producer.flush()

            # Mock Google API client
            with patch("google_drive_worker.google_client.GoogleDriveClient") as MockClient:
                mock_client = AsyncMock()
                MockClient.return_value = mock_client

                # Mock API response for changed file
                mock_client.get_file_metadata.return_value = sample_google_file
                mock_client.list_permissions.return_value = {
                    "permissions": sample_google_file["permissions"]
                }

                # Create and start worker
                worker = GoogleDriveWorker(
                    kafka_config={
                        "bootstrap_servers": kafka_config["bootstrap_servers"],
                        "consumer_group_id": "webhook-worker",
                        "auto_offset_reset": "earliest",
                        "enable_auto_commit": False,
                    },
                    worker_config={
                        "integration_id": "google-drive",
                    }
                )

                # Subscribe to webhook topic
                await worker.consumer.subscribe([webhook_topic])

                # Process webhook
                process_task = asyncio.create_task(worker.process_single_message())
                await asyncio.wait_for(process_task, timeout=10.0)

                # Consume from ingestion topic
                messages = []
                async for msg in consumer:
                    messages.append(msg.value)
                    if len(messages) >= 1:
                        break

                # Verify ingestion message from webhook
                assert len(messages) == 1
                ingestion_msg = messages[0]
                assert ingestion_msg["integration_id"] == "google-drive"
                assert ingestion_msg["data_type"] == "file"

        finally:
            await consumer.stop()

    @pytest.mark.asyncio
    async def test_error_handling_dlq(
        self,
        kafka_container: KafkaContainer,
        kafka_config: Dict[str, Any],
        kafka_producer: AIOKafkaProducer,
        sample_trigger_message: Dict[str, Any]
    ):
        """Test that errors are properly sent to integration.errors topic."""
        trigger_topic = "integration.trigger"
        error_topic = "integration.errors"

        # Create consumer for error topic
        error_consumer = AIOKafkaConsumer(
            error_topic,
            bootstrap_servers=kafka_config["bootstrap_servers"],
            group_id="test-error-consumer",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await error_consumer.start()

        try:
            # Produce trigger message
            await kafka_producer.send(
                trigger_topic,
                key=sample_trigger_message["integration_connection_id"],
                value=sample_trigger_message
            )
            await kafka_producer.flush()

            # Mock Google API client to raise an error
            with patch("google_drive_worker.google_client.GoogleDriveClient") as MockClient:
                mock_client = AsyncMock()
                MockClient.return_value = mock_client

                # Mock API to raise an error
                mock_client.list_files.side_effect = Exception("API Error: Rate limit exceeded")

                # Create and start worker
                worker = GoogleDriveWorker(
                    kafka_config={
                        "bootstrap_servers": kafka_config["bootstrap_servers"],
                        "consumer_group_id": "error-test-worker",
                        "auto_offset_reset": "earliest",
                        "enable_auto_commit": False,
                    },
                    worker_config={
                        "integration_id": "google-drive",
                    }
                )

                # Process message (should fail)
                try:
                    await worker.process_single_message()
                except Exception:
                    pass  # Expected to fail

                # Consume from error topic
                error_messages = []
                async for msg in error_consumer:
                    error_messages.append(msg.value)
                    if len(error_messages) >= 1:
                        break

                # Verify error message
                assert len(error_messages) == 1
                error_msg = error_messages[0]

                assert error_msg["integration_id"] == "google-drive"
                assert error_msg["integration_connection_id"] == "conn_abc123"
                assert "error" in error_msg
                assert "Rate limit exceeded" in str(error_msg["error"])
                assert error_msg["error_type"] == "processing_error"

        finally:
            await error_consumer.stop()

    @pytest.mark.asyncio
    async def test_offset_commit(
        self,
        kafka_container: KafkaContainer,
        kafka_config: Dict[str, Any],
        kafka_producer: AIOKafkaProducer,
        sample_trigger_message: Dict[str, Any],
        sample_google_file: Dict[str, Any]
    ):
        """Test that offsets are properly committed after successful processing."""
        trigger_topic = "integration.trigger"

        # Produce multiple messages
        for i in range(3):
            msg = sample_trigger_message.copy()
            msg["message_id"] = f"msg_{i}"
            await kafka_producer.send(
                trigger_topic,
                key=msg["integration_connection_id"],
                value=msg
            )
        await kafka_producer.flush()

        # Mock Google API
        with patch("google_drive_worker.google_client.GoogleDriveClient") as MockClient:
            mock_client = AsyncMock()
            MockClient.return_value = mock_client
            mock_client.list_files.return_value = {"files": [sample_google_file], "nextPageToken": None}
            mock_client.get_file_metadata.return_value = sample_google_file
            mock_client.list_permissions.return_value = {"permissions": []}

            # Create worker
            worker = GoogleDriveWorker(
                kafka_config={
                    "bootstrap_servers": kafka_config["bootstrap_servers"],
                    "consumer_group_id": "offset-test-worker",
                    "auto_offset_reset": "earliest",
                    "enable_auto_commit": False,
                },
                worker_config={
                    "integration_id": "google-drive",
                }
            )

            # Process messages
            for _ in range(3):
                await worker.process_single_message()

            # Verify offsets were committed
            committed = await worker.consumer.committed(
                worker.consumer.assignment().pop()
            )
            assert committed is not None
            assert committed >= 3  # Should have processed at least 3 messages

    @pytest.mark.asyncio
    async def test_concurrent_processing(
        self,
        kafka_container: KafkaContainer,
        kafka_config: Dict[str, Any],
        kafka_producer: AIOKafkaProducer,
        sample_trigger_message: Dict[str, Any],
        sample_google_file: Dict[str, Any]
    ):
        """Test concurrent processing of multiple connections."""
        trigger_topic = "integration.trigger"
        ingestion_topic = "ingestion.data"

        # Create consumer for ingestion topic
        consumer = AIOKafkaConsumer(
            ingestion_topic,
            bootstrap_servers=kafka_config["bootstrap_servers"],
            group_id="test-concurrent-consumer",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await consumer.start()

        try:
            # Produce messages for different connections
            connections = ["conn_1", "conn_2", "conn_3"]
            for conn_id in connections:
                msg = sample_trigger_message.copy()
                msg["integration_connection_id"] = conn_id
                msg["message_id"] = f"msg_{conn_id}"
                await kafka_producer.send(
                    trigger_topic,
                    key=conn_id,
                    value=msg
                )
            await kafka_producer.flush()

            # Mock Google API
            with patch("google_drive_worker.google_client.GoogleDriveClient") as MockClient:
                mock_client = AsyncMock()
                MockClient.return_value = mock_client

                # Different files for each connection
                async def list_files_side_effect(*args, **kwargs):
                    # Simulate some delay
                    await asyncio.sleep(0.1)
                    return {"files": [sample_google_file], "nextPageToken": None}

                mock_client.list_files.side_effect = list_files_side_effect
                mock_client.get_file_metadata.return_value = sample_google_file
                mock_client.list_permissions.return_value = {"permissions": []}

                # Create worker with concurrent connections
                worker = GoogleDriveWorker(
                    kafka_config={
                        "bootstrap_servers": kafka_config["bootstrap_servers"],
                        "consumer_group_id": "concurrent-worker",
                        "auto_offset_reset": "earliest",
                        "enable_auto_commit": False,
                    },
                    worker_config={
                        "integration_id": "google-drive",
                        "max_concurrent_connections": 3,
                    }
                )

                # Process messages concurrently
                tasks = []
                for _ in range(3):
                    task = asyncio.create_task(worker.process_single_message())
                    tasks.append(task)

                # Wait for all to complete
                await asyncio.gather(*tasks)

                # Consume from ingestion topic
                messages = []
                deadline = asyncio.get_event_loop().time() + 5
                while len(messages) < 3 and asyncio.get_event_loop().time() < deadline:
                    try:
                        msg = await asyncio.wait_for(
                            consumer.getone(),
                            timeout=1.0
                        )
                        messages.append(msg.value)
                    except asyncio.TimeoutError:
                        break

                # Verify all connections were processed
                assert len(messages) >= 3
                processed_connections = {msg["integration_connection_id"] for msg in messages}
                assert len(processed_connections) == 3

        finally:
            await consumer.stop()