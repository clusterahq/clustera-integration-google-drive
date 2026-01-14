"""Main worker orchestration for Google Drive integration.

This module contains the core worker logic that:
1. Consumes from Kafka topics (integration.trigger, webhook.raw)
2. Processes messages through appropriate handlers
3. Produces normalized data to ingestion.data
4. Handles errors and produces to integration.errors
"""

import asyncio
import json
import logging
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
from clustera_integration_toolkit.message import IncomingMessageBuilder, ErrorMessageBuilder
from clustera_integration_toolkit import topics

from .config import Settings
from .handlers.init import InitHandler
from .handlers.fetch import FetchHandler
from .handlers.write import WriteHandler
from .handlers.teardown import TeardownHandler
from .handlers.webhook import GoogleDriveWebhookHandler
from .handlers.capability import GoogleDriveCapabilityHandler
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

    def __init__(self, settings: Settings, provider_name: Optional[str] = None):
        """Initialize the worker.

        Args:
            settings: Application settings
            provider_name: Provider name (optional, default None)
        """
        self.settings = settings
        self.logger = self._setup_logger()
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.provider_name = provider_name or "google-drive"

        # Topic names from registry (respects ENVIRONMENT prefix)
        self.command_topic = topics.worker_topic("google-drive")
        self.output_topic = topics.outbound_data
        self.error_topic = topics.errors

        # Kafka clients (initialized in start)
        self.consumer: Optional[KafkaConsumer] = None
        self.producer: Optional[KafkaProducer] = None

        # Idempotency cache (in-memory LRU)
        self.idempotency_cache = IdempotencyCache(
            max_size=settings.worker.idempotency_cache_size,
            ttl_seconds=settings.worker.idempotency_cache_ttl_seconds,
        )

        # Handlers (5-handler pattern)
        self.handlers = [
            InitHandler(settings.google_drive),
            FetchHandler(
                settings.google_drive,
                storage_config=settings.storage,
            ),
            WriteHandler(settings.google_drive),
            TeardownHandler(settings.google_drive),
            GoogleDriveWebhookHandler(
                settings.google_drive,
                storage_config=settings.storage,
                provider_name=provider_name,
            ),
        ]

        # Capability handler (processes method-based routing, not action-based)
        self.capability_handler = GoogleDriveCapabilityHandler()

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
        """Start the worker and begin consuming messages."""
        self.logger.info("Starting Google Drive worker")

        await self._init_kafka_clients()

        # Publish capability manifest to S3 on startup
        await self._publish_capabilities()

        self.running = True

        # Setup signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._handle_shutdown_signal)

        try:
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

        # Initialize consumer
        self.consumer = KafkaConsumer(
            config=kafka_config,
            topics=[self.command_topic],
            client_id="google-drive-worker-consumer",
        )

        # Initialize producer
        self.producer = KafkaProducer(
            config=kafka_config,
            client_id="google-drive-worker-producer",
        )

    async def _publish_capabilities(self) -> None:
        """Publish capability manifest to S3 on startup.

        If S3 is not configured or in mock mode, this is a no-op.
        Errors during publishing are logged but do not fail startup.
        """
        # Skip if in mock mode
        if self.settings.google_drive.mock_mode or self.settings.storage.mock_mode:
            self.logger.info("[MOCK] Skipping capability manifest publishing")
            return

        # Check if S3 is configured (requires AWS credentials or endpoint)
        import os
        s3_configured = bool(
            os.getenv("S3_BUCKET")
            and (os.getenv("S3_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID"))
        )

        if not s3_configured:
            self.logger.info(
                "S3 not configured, skipping capability manifest publishing",
                hint="Set S3_BUCKET and S3_ACCESS_KEY_ID to enable",
            )
            return

        try:
            from clustera_integration_toolkit.capability import CapabilityPublisher
            from clustera_integration_toolkit.storage import (
                ObjectStorageClient,
                ObjectStorageConfig,
            )

            # Initialize S3 client using standard S3_* env vars
            config = ObjectStorageConfig.from_env()
            s3_client = ObjectStorageClient(config)

            # Initialize publisher with _meta/capabilities prefix
            publisher = CapabilityPublisher(
                s3_client=s3_client,
                bucket=config.bucket,
                prefix="_meta/capabilities",
            )

            # Get manifest and publish
            manifest = self.capability_handler.get_capability_manifest()
            result = await publisher.publish(manifest)

            self.logger.info(
                "Capability manifest published to S3",
                s3_key=result["s3_key"],
                provider=result["provider"],
                version=result["version"],
                size_bytes=result["size_bytes"],
            )

        except Exception as e:
            self.logger.warning(
                "Failed to publish capability manifest to S3",
                error=str(e),
                hint="Worker will continue without S3 publishing",
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

    def _extract_connection_id(self, message: dict[str, Any]) -> str:
        """Extract connection ID from JSON-RPC header or message root.

        Args:
            message: The message payload

        Returns:
            Connection ID string
        """
        # JSON-RPC 2.0 format: params.header.parameters.integration_connection_id
        if message.get("jsonrpc") == "2.0":
            params = message.get("params", {})
            header = params.get("header", {})
            header_params = header.get("parameters", {})
            conn_id = header_params.get("integration_connection_id")
            if conn_id:
                return conn_id

        # Legacy format: message root
        return message.get("integration_connection_id", "unknown")

    def _extract_integration_id(self, message: dict[str, Any]) -> str | None:
        """Extract integration ID from JSON-RPC header or message root.

        Args:
            message: The message payload

        Returns:
            Integration ID string or None if not found
        """
        # JSON-RPC 2.0 format: params.header.parameters.integration_provider_name
        if message.get("jsonrpc") == "2.0":
            params = message.get("params", {})
            header = params.get("header", {})
            header_params = header.get("parameters", {})
            provider_name = header_params.get("integration_provider_name")
            if provider_name:
                return provider_name

        # Legacy format: message root
        return message.get("integration_id")

    async def _process_single_message(self, message: KafkaMessage) -> None:
        """Process a single Kafka message.

        Args:
            message: Kafka message from consumer (toolkit KafkaMessage)
        """
        topic = message.topic
        key = message.key
        value = message.value
        headers = message.headers

        # Extract connection ID using helper (supports JSON-RPC 2.0 and legacy)
        connection_id = self._extract_connection_id(value)

        # Check if we should process this message (supports JSON-RPC 2.0 and legacy)
        integration_id = self._extract_integration_id(value)
        if integration_id and integration_id != self.settings.worker.integration_id:
            # Not for us, skip with logging
            self.logger.debug(
                "Skipping message for different integration",
                received_integration_id=integration_id,
                expected_integration_id=self.settings.worker.integration_id,
                connection_id=connection_id,
            )
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
        before_sleep=before_sleep_log(structlog.get_logger(), logging.INFO),
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
            # Plan 35/36: Pass connection_config for snowball extraction
            await self._produce_record(record, connection_config=connection_config)
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
        """Handle a message based on its method/action.

        Routing priority:
        1. JSON-RPC 2.0 method field (for all JSON-RPC requests)
        2. Legacy action field (for backward compatibility)

        Args:
            topic: Kafka topic name (for logging)
            key: Message key (connection_id)
            value: Message value
            headers: Message headers
            connection_id: Integration connection ID
        """
        start_time = time.time()

        # Extract routing fields
        action = value.get("action", "unknown")
        method = value.get("method")
        is_jsonrpc = value.get("jsonrpc") == "2.0"

        self.logger.info(
            "Handling message",
            topic=topic,
            action=action,
            method=method,
            message_id=value.get("message_id") or value.get("id"),
            connection_id=connection_id,
            jsonrpc_format=is_jsonrpc,
        )

        # Handle capability discovery requests (JSON-RPC method-based)
        if method == "clustera.integration.capability.describe":
            try:
                response = await self.capability_handler.handle_capability_request(value)
                await self._produce_capability_response(response, connection_id)
                self.metrics["messages_processed"] += 1
                return
            except Exception as e:
                self.logger.error(
                    "Capability request failed",
                    error=str(e),
                    connection_id=connection_id,
                )
                error_response = self.capability_handler.build_error_response(
                    value,
                    code=-32603,
                    error_message="Internal error",
                    details=str(e),
                )
                await self._produce_capability_response(error_response, connection_id)
                return

        try:
            # Handle "pending_lookup" case for webhooks that don't have connection_id
            if connection_id == "pending_lookup":
                self.logger.info(
                    "Connection ID is pending_lookup, handler must resolve from payload",
                    method=method,
                    action=action,
                )
                connection_config: Dict[str, Any] = {}
            else:
                # Fetch connection configuration from Control Plane
                # TODO: Implement actual Control Plane call
                connection_config = {
                    "connection_id": connection_id,
                    "access_token": "placeholder_access_token",
                }

            # Route to handler based on method (JSON-RPC) or action (legacy)
            handler = None

            for h in self.handlers:
                if h.can_handle(value):
                    handler = h
                    break

            if not handler:
                self.logger.warning(
                    "No handler for message",
                    topic=topic,
                    action=action,
                    method=method,
                    integration_id=value.get("integration_id"),
                )
                await self.consumer.commit()
                return

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

    async def _produce_capability_response(
        self,
        response: Dict[str, Any],
        connection_id: str,
    ) -> None:
        """Produce capability response to integrations-incoming-records.

        Args:
            response: JSON-RPC 2.0 response payload
            connection_id: Integration connection ID (for partition key)
        """
        topic = self.output_topic
        key = connection_id

        await self.producer.send(
            topic=topic,
            key=key,
            value=response,
        )

        self.logger.info(
            "Capability response produced",
            topic=topic,
            key=key,
            response_id=response.get("id"),
        )

    async def _produce_record(
        self,
        record: Dict[str, Any],
        connection_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Produce a record to ingestion.data topic with idempotency check.

        Args:
            record: Normalized record to produce
            connection_config: Connection configuration (for snowball in Plan 35/36)
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

        # Plan 35/36: Extract snowball from connection_config
        config = connection_config or {}
        snowball = config.get("snowball_clusterspace")

        # Build message using IncomingMessageBuilder for proper JSON-RPC 2.0 format
        message = IncomingMessageBuilder.build_from_worker(
            customer_id=record.get("customer_id", "unknown"),
            integration_provider_name="google-drive",
            integration_connection_id=key,
            payload=record,
            idempotency_key=idempotency_key,
            event_type=record.get("resource_type", "fetch"),
            snowball=snowball,  # Plan 35/36: Include snowball in header
        )

        # Add headers (toolkit producer expects Dict[str, str])
        headers = {
            "source": self.settings.service_name,
            "produced_at": datetime.utcnow().isoformat(),
            "idempotency_key": idempotency_key,
        }

        await self.producer.send(
            topic=topic,
            key=key,
            value=message,
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
        """Produce error to integration.errors topic using ErrorMessageBuilder.

        Args:
            error: The error that occurred
            original_message: Original message being processed
            connection_id: Connection ID
            retriable: Whether error is retriable (None = auto-detect)
        """
        # Determine if retriable if not specified
        if retriable is None:
            retriable = isinstance(error, RetriableError)

        # Extract error details from IntegrationError
        error_category = "unknown"
        error_details: Dict[str, Any] = {}
        if isinstance(error, IntegrationError):
            error_category = error.category
            error_details = error.details

        # Build context with error details
        context: Dict[str, Any] = {
            "error_category": error_category,
            "exception_type": type(error).__name__,
        }
        if error_details:
            context["error_details"] = error_details

        # Build error message using ErrorMessageBuilder
        error_message = ErrorMessageBuilder.build(
            customer_id=original_message.get("customer_id", "unknown"),
            integration_provider_name=self.provider_name,
            integration_connection_id=connection_id,
            error_type="retriable" if retriable else "terminal",
            error_code=type(error).__name__.upper(),
            error_message=str(error),
            source_message_id=original_message.get("message_id") or original_message.get("id"),
            context=context,
        )

        # Use error topic (toolkit/dataplane will handle DLQ routing)
        topic = self.error_topic

        # Partition key is provider_name for error topic (enables provider-specific monitoring)
        await self.producer.send(
            topic=topic,
            key=self.provider_name,
            value=error_message,
        )

        self.logger.info(
            "message_produced_to_kafka",
            topic=topic,
            key=self.provider_name,
            record_type="error",
            error_type=type(error).__name__,
            error_category=error_category,
            message_id=original_message.get("message_id", "unknown"),
            retriable=retriable
        )

        self.metrics["errors_produced"] += 1

    def _handle_shutdown_signal(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating shutdown")
        self.running = False