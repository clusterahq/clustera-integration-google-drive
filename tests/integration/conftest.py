"""Shared fixtures for integration tests."""

import asyncio
import json
from typing import Any, AsyncGenerator, Dict, Generator

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an event loop for the entire test session."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    """Start Kafka container for the test session."""
    with KafkaContainer(image="confluentinc/cp-kafka:7.5.0") as kafka:
        yield kafka


@pytest.fixture(scope="function")
def kafka_config(kafka_container: KafkaContainer) -> Dict[str, Any]:
    """Kafka configuration for tests."""
    return {
        "bootstrap_servers": kafka_container.get_bootstrap_server(),
        "consumer_group_id": f"test-worker-{asyncio.current_task().get_name()}",
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "max_poll_interval_ms": 300000,
        "session_timeout_ms": 10000,
    }


@pytest.fixture
async def kafka_producer(kafka_config: Dict[str, Any]) -> AsyncGenerator[AIOKafkaProducer, None]:
    """Create a Kafka producer for testing."""
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        enable_idempotence=True,
        compression_type="gzip",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@pytest.fixture
async def kafka_consumer(kafka_config: Dict[str, Any]) -> AsyncGenerator[AIOKafkaConsumer, None]:
    """Create a Kafka consumer for testing."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        group_id=kafka_config["consumer_group_id"],
        auto_offset_reset=kafka_config["auto_offset_reset"],
        enable_auto_commit=kafka_config["enable_auto_commit"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()


@pytest.fixture
def sample_trigger_message() -> Dict[str, Any]:
    """Sample trigger message for testing."""
    return {
        "message_id": "msg_12345",
        "customer_id": "cust_xyz",
        "integration_id": "google-drive",
        "integration_connection_id": "conn_abc123",
        "trigger_type": "scheduled",
        "trigger_metadata": {
            "scheduled_at": "2025-01-15T10:00:00Z",
            "frequency": "hourly"
        },
        "connection_config": {
            "refresh_token": "test_refresh_token",
            "page_size": 50,
            "include_shared_drives": True,
            "full_sync": False
        }
    }


@pytest.fixture
def sample_webhook_message() -> Dict[str, Any]:
    """Sample webhook message for testing."""
    return {
        "message_id": "webhook_12345",
        "customer_id": "cust_xyz",
        "integration_id": "google-drive",
        "integration_connection_id": "conn_abc123",
        "webhook_type": "file.change",
        "webhook_payload": {
            "id": "test_channel_id",
            "resourceId": "test_resource_id",
            "resourceUri": "https://www.googleapis.com/drive/v3/files/test_file",
            "token": "test_token",
            "expiration": "1234567890000"
        },
        "received_at": "2025-01-15T10:00:00Z"
    }


@pytest.fixture
def sample_google_file() -> Dict[str, Any]:
    """Sample Google Drive file for testing."""
    return {
        "id": "file_123",
        "name": "test_document.pdf",
        "mimeType": "application/pdf",
        "size": "1024",
        "createdTime": "2025-01-01T10:00:00Z",
        "modifiedTime": "2025-01-15T10:00:00Z",
        "owners": [{"emailAddress": "owner@example.com"}],
        "permissions": [
            {
                "id": "perm_1",
                "type": "user",
                "emailAddress": "user@example.com",
                "role": "writer"
            }
        ],
        "webViewLink": "https://drive.google.com/file/d/file_123/view",
        "webContentLink": "https://drive.google.com/uc?id=file_123&export=download"
    }