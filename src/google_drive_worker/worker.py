"""Main worker orchestration for Google Drive integration.

This module contains the core worker logic that:
1. Consumes from Kafka topics (integration.trigger, webhook.raw)
2. Processes messages through appropriate handlers
3. Produces normalized data to ingestion.data
4. Handles errors and produces to integration.errors
"""

import asyncio
import json
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Set
from datetime import datetime
from contextlib import asynccontextmanager
import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from clustera_integration_toolkit.idempotency import IdempotencyCache
from clustera_integration_toolkit.kafka import (
    KafkaProducer,
    KafkaConsumer,
    KafkaConfig,
    KafkaMessage,
)

from .config import Settings
from .handlers.trigger import GoogleDriveTriggerHandler
from .handlers.webhook import GoogleDriveWebhookHandler
from .utils.errors import (
    IntegrationError,
    RetriableError,
    TerminalError,
    RateLimitError,
)


class GoogleDriveWorker:
    """Main worker orchestration for Google Drive integration.

    Manages Kafka consumption, message processing, and production.
    """

    def __init__(self, settings: Settings):
        """Initialize the worker.

        Args:
            settings: Application settings
        """
        self.settings = settings
        self.logger = self._setup_logger()
        self.running = False
        self.shutdown_event = asyncio.Event()

        # Kafka clients (initialized in start)
        self.consumer: Optional[KafkaConsumer] = None
        self.producer: Optional[KafkaProducer] = None

        # Idempotency cache (in-memory LRU)
        self.idempotency_cache = IdempotencyCache(
            max_size=settings.worker.idempotency_cache_size,
            ttl_seconds=settings.worker.idempotency_cache_ttl_seconds,
        )

        # Handlers
        self.trigger_handler = GoogleDriveTriggerHandler(
            api_config=settings.google_drive,
            logger=self.logger,
        )
        self.webhook_handler = GoogleDriveWebhookHandler(
            api_config=settings.google_drive,
            logger=self.logger,
        )

        # Concurrency control
        self.active_connections: Set[str] = set()
        self.connection_semaphore = asyncio.Semaphore(
            settings.worker.max_concurrent_connections
        )

        # Enhanced metrics
        self.metrics = {
            # Counters
            "messages_processed": 0,
            "messages_failed": 0,
            "records_produced": 0,
            "errors_produced": 0,
            "errors_retriable": 0,
            "errors_terminal": 0,
            "duplicates_skipped": 0,
            "payloads_offloaded_to_s3": 0,

            # Gauges
            "processing_time_seconds": 0.0,
            "api_calls_total": 0,
            "cache_hit_rate": 0.0,
        }

    def _setup_logger(self) -> structlog.BoundLogger:
        """Setup structured logging."""
        import logging

        # Configure Python's logging backend to output to stdout
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=getattr(logging, self.settings.logging.level.upper(), logging.INFO),
        )

        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer() if self.settings.logging.format == "json"
                else structlog.dev.ConsoleRenderer(),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        return structlog.get_logger().bind(
            service=self.settings.service_name,
            version=self.settings.service_version,
            environment=self.settings.environment,
        )

    async def start(self) -> None:
        """Start the worker and begin processing messages."""
        self.logger.info("Starting Google Drive worker")
        self.running = True

        # Setup signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._handle_shutdown_signal)

        try:
            # Initialize Kafka clients
            await self._init_kafka_clients()

            # Start consumer
            await self.consumer.start()
            self.logger.info("Kafka consumer started")

            # Start producer
            await self.producer.start()
            self.logger.info("Kafka producer started")

            # Main processing loop
            await self._process_messages()

        except Exception as e:
            self.logger.error("Worker failed", error=str(e), exc_info=True)
            raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        self.logger.info("Stopping Google Drive worker")
        self.running = False

        # Wait for active connections to complete
        if self.active_connections:
            self.logger.info(
                "Waiting for active connections",
                count=len(self.active_connections),
            )
            await asyncio.sleep(5)  # Give time to complete

        # Close Kafka clients
        if self.consumer:
            await self.consumer.stop()
            self.logger.info("Kafka consumer stopped")

        if self.producer:
            await self.producer.stop()
            self.logger.info("Kafka producer stopped")

        self.logger.info(
            "Worker stopped",
            metrics=self.metrics,
        )

    async def _init_kafka_clients(self) -> None:
        """Initialize Kafka consumer and producer using toolkit."""
        # Build toolkit KafkaConfig from settings
        kafka_config = KafkaConfig(
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            consumer_group_id=self.settings.kafka.consumer_group_id,
            auto_offset_reset=self.settings.kafka.auto_offset_reset,
        )

        # Topics to subscribe to
        topics = [
            "integration.trigger",  # Polling triggers from Control Plane
            "webhook.raw",  # Push notifications from Google Drive
        ]

        # Initialize consumer
        self.consumer = KafkaConsumer(
            config=kafka_config,
            topics=topics,
            client_id=f"{self.settings.service_name}-consumer",
        )

        # Initialize producer
        self.producer = KafkaProducer(
            config=kafka_config,
            client_id=f"{self.settings.service_name}-producer",
        )

    async def _process_messages(self) -> None:
        """Main message processing loop."""
        self.logger.info("Starting message processing loop")

        while self.running:
            try:
                # Fetch messages with timeout (toolkit returns List[KafkaMessage])
                messages = await self.consumer.poll_messages(
                    max_messages=100,
                    timeout_ms=1000,
                )

                if not messages:
                    continue

                # Process each message
                for message in messages:
                    await self._process_single_message(message)

            except asyncio.CancelledError:
                self.logger.info("Processing loop cancelled")
                break
            except Exception as e:
                self.logger.error(
                    "Error in processing loop",
                    error=str(e),
                    exc_info=True,
                )
                await asyncio.sleep(1)  # Backoff on error

    async def _process_single_message(self, message: KafkaMessage) -> None:
        """Process a single Kafka message.

        Args:
            message: Kafka message from consumer (toolkit KafkaMessage)
        """
        topic = message.topic
        key = message.key
        value = message.value
        headers = message.headers

        # Extract connection ID for concurrency control
        connection_id = value.get("integration_connection_id", "unknown")

        # Check if we should process this message
        integration_id = value.get("integration_id")
        if integration_id != self.settings.worker.integration_id:
            # Not for us, skip
            await self.consumer.commit()
            return

        # Concurrency control per connection
        async with self.connection_semaphore:
            if connection_id in self.active_connections:
                self.logger.warning(
                    "Connection already being processed",
                    connection_id=connection_id,
                )
                return

            self.active_connections.add(connection_id)
            try:
                await self._handle_message(
                    topic,
                    key,
                    value,
                    headers,
                    connection_id,
                )
                self.metrics["messages_processed"] += 1
            except Exception as e:
                self.metrics["messages_failed"] += 1
                self.logger.error(
                    "Failed to process message",
                    error=str(e),
                    topic=topic,
                    connection_id=connection_id,
                    exc_info=True,
                )
            finally:
                self.active_connections.discard(connection_id)

    @retry(
        retry=retry_if_exception_type(RetriableError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2.0, max=60.0),
        before_sleep=before_sleep_log(structlog.get_logger(), structlog.INFO),
    )
    async def _process_with_retry(
        self,
        handler: Any,
        message: Dict[str, Any],
        connection_config: Dict[str, Any],
    ) -> int:
        """Process message with automatic retries for retriable errors.

        Args:
            handler: Handler instance
            message: Message to process
            connection_config: Connection configuration

        Returns:
            Number of records produced
        """
        record_count = 0
        async for record in handler.process_message(message, connection_config):
            await self._produce_record(record)
            record_count += 1
            self.metrics["records_produced"] += 1
        return record_count

    async def _handle_message(
        self,
        topic: str,
        key: str,
        value: Dict[str, Any],
        headers: Dict[str, Any],
        connection_id: str,
    ) -> None:
        """Handle a message based on its topic.

        Args:
            topic: Kafka topic name
            key: Message key (connection_id)
            value: Message value
            headers: Message headers
            connection_id: Integration connection ID
        """
        start_time = time.time()
        self.logger.info(
            "Handling message",
            topic=topic,
            connection_id=connection_id,
            message_id=value.get("message_id"),
        )

        try:
            # Select handler based on topic
            if topic == "integration.trigger":
                handler = self.trigger_handler
            elif topic == "webhook.raw":
                handler = self.webhook_handler
            else:
                raise TerminalError(f"Unknown topic: {topic}")

            # Check if handler can process
            if not await handler.can_handle(value):
                self.logger.info(
                    "Handler cannot process message",
                    handler=handler.__class__.__name__,
                )
                await self.consumer.commit()
                return

            # Fetch connection configuration
            connection_config = await handler.fetch_connection_config(connection_id)

            # Process with retries
            record_count = await self._process_with_retry(
                handler, value, connection_config
            )

            # Track processing time
            duration = time.time() - start_time
            self.metrics["processing_time_seconds"] += duration

            self.logger.info(
                "Message processed successfully",
                connection_id=connection_id,
                record_count=record_count,
                processing_time_seconds=duration,
            )

            # Commit offset after successful processing
            await self.consumer.commit()

        except RetriableError as e:
            # Retriable error after all retries exhausted
            self.logger.warning(
                "Retriable error occurred, will retry on next poll",
                error=str(e),
                connection_id=connection_id,
                retry_after=getattr(e, "retry_after", None),
            )
            # Don't commit - message will be retried on next poll
            await self._produce_error(e, value, connection_id, retriable=True)
            self.metrics["errors_retriable"] += 1

        except TerminalError as e:
            # Terminal error - send to DLQ and commit
            self.logger.error(
                "Terminal error occurred, moving to DLQ",
                error=str(e),
                connection_id=connection_id,
            )
            await self._produce_error(e, value, connection_id, retriable=False)
            await self.consumer.commit()  # Skip this message
            self.metrics["errors_terminal"] += 1

        except Exception as e:
            # Unexpected error - treat as terminal
            self.logger.exception(
                "Unexpected error occurred, moving to DLQ",
                error=str(e),
                connection_id=connection_id,
            )
            await self._produce_error(e, value, connection_id, retriable=False)
            await self.consumer.commit()  # Skip this message
            self.metrics["errors_terminal"] += 1

        finally:
            # Track processing time even on failure
            if start_time:
                duration = time.time() - start_time
                self.metrics["processing_time_seconds"] += duration

    async def _produce_record(self, record: Dict[str, Any]) -> None:
        """Produce a record to ingestion.data topic with idempotency check.

        Args:
            record: Normalized record to produce
        """
        idempotency_key = record.get("idempotency_key")

        # Check idempotency cache (atomic check-and-set)
        if not self.idempotency_cache.check_and_set(idempotency_key):
            # Returns False if key already exists (duplicate)
            self.logger.debug(
                "Skipping duplicate record",
                idempotency_key=idempotency_key,
                resource_type=record.get("resource_type"),
                resource_id=record.get("resource_id"),
            )
            self.metrics["duplicates_skipped"] += 1
            return

        topic = "ingestion.data"
        key = record["integration_connection_id"]

        # Check payload size for S3 offloading
        payload_size = len(json.dumps(record).encode("utf-8"))
        if payload_size > self.settings.worker.s3_payload_threshold_bytes:
            # TODO: Implement S3 offloading in next step
            self.logger.warning(
                "Payload exceeds threshold, would offload to S3",
                size=payload_size,
                threshold=self.settings.worker.s3_payload_threshold_bytes,
            )
            self.metrics["payloads_offloaded_to_s3"] += 1

        # Add headers (toolkit producer expects Dict[str, str])
        headers = {
            "source": self.settings.service_name,
            "produced_at": datetime.utcnow().isoformat(),
            "idempotency_key": record["idempotency_key"],
        }

        await self.producer.send(
            topic=topic,
            key=key,
            value=record,
            headers=headers,
        )

        # Note: idempotency key already added to cache via check_and_set() above

        self.logger.info(
            "message_produced_to_kafka",
            topic=topic,
            key=key,
            record_type="data",
            resource_type=record.get("resource_type"),
            resource_id=record.get("resource_id"),
            idempotency_key=idempotency_key,
            message_id=record.get("message_id"),
            headers=headers
        )

    async def _produce_error(
        self,
        error: Exception,
        original_message: Dict[str, Any],
        connection_id: str,
        retriable: Optional[bool] = None,
    ) -> None:
        """Produce error to integration.errors topic or DLQ.

        Args:
            error: The error that occurred
            original_message: Original message being processed
            connection_id: Connection ID
            retriable: Whether error is retriable (None = auto-detect)
        """
        # Determine if retriable if not specified
        if retriable is None:
            retriable = isinstance(error, RetriableError)

        # Use DLQ for terminal errors, regular errors for retriable
        topic = "integration.errors" if retriable else "integration.errors.dlq"

        error_record = {
            "message_id": original_message.get("message_id", "unknown"),
            "customer_id": original_message.get("customer_id", "unknown"),
            "integration_id": self.settings.worker.integration_id,
            "integration_connection_id": connection_id,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
                "retriable": retriable,
            },
            "original_message": original_message,
            "occurred_at": datetime.utcnow().isoformat() + "Z",
        }

        if isinstance(error, IntegrationError):
            error_record["error"].update(error.to_dict())

        await self.producer.send(
            topic=topic,
            key=connection_id,
            value=error_record,
        )

        self.logger.info(
            "message_produced_to_kafka",
            topic=topic,
            key=connection_id,
            record_type="error",
            error_type=type(error).__name__,
            message_id=error_record.get("message_id"),
            retriable=retriable
        )

        self.metrics["errors_produced"] += 1

    def _handle_shutdown_signal(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating shutdown")
        self.running = False