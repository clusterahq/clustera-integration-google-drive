"""Base handler interface for integration workers.

This abstract base class defines the contract that all integration handlers
must implement. It's designed to be provider-agnostic and reusable across
all integration workers.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, AsyncIterator
from datetime import datetime
import structlog

from ..utils.errors import IntegrationError


class BaseIntegrationHandler(ABC):
    """Abstract base class for integration handlers.

    This defines the interface that both trigger handlers and webhook handlers
    must implement. Each handler is responsible for:
    1. Processing incoming messages (triggers or webhooks)
    2. Fetching data from the provider
    3. Normalizing data to the canonical format
    4. Handling errors and retries
    """

    def __init__(
        self,
        integration_id: str,
        logger: Optional[structlog.BoundLogger] = None,
    ):
        """Initialize handler.

        Args:
            integration_id: The integration identifier (e.g., "google-drive", "github")
            logger: Structured logger instance
        """
        self.integration_id = integration_id
        self.logger = logger or structlog.get_logger()

    @abstractmethod
    async def can_handle(self, message: Dict[str, Any]) -> bool:
        """Check if this handler can process the given message.

        Args:
            message: The Kafka message value (deserialized JSON)

        Returns:
            True if this handler can process the message, False otherwise
        """
        pass

    @abstractmethod
    async def process_message(
        self,
        message: Dict[str, Any],
        connection_config: Dict[str, Any],
    ) -> AsyncIterator[Dict[str, Any]]:
        """Process a message and yield normalized records.

        This is the main processing method. It should:
        1. Parse the message
        2. Fetch data from the provider (if needed)
        3. Normalize the data
        4. Yield normalized records for production to ingestion.data

        Args:
            message: The Kafka message value
            connection_config: Configuration for the connection (credentials, settings)

        Yields:
            Normalized records ready for ingestion.data topic

        Raises:
            RetriableError: For temporary failures that should be retried
            TerminalError: For permanent failures that should go to DLQ
        """
        pass

    @abstractmethod
    def generate_idempotency_key(
        self,
        connection_id: str,
        resource_type: str,
        resource_id: str,
    ) -> str:
        """Generate a deterministic idempotency key.

        Pattern: {provider}:{connection_id}:{resource_type}:{resource_id}

        Args:
            connection_id: The integration connection ID
            resource_type: Type of resource (e.g., "file", "folder")
            resource_id: Unique ID of the resource

        Returns:
            Deterministic idempotency key
        """
        pass

    async def fetch_connection_config(
        self,
        connection_id: str,
    ) -> Dict[str, Any]:
        """Fetch connection configuration from Control Plane.

        This is a common method that can be overridden if needed.

        Args:
            connection_id: The integration connection ID

        Returns:
            Connection configuration including credentials
        """
        # TODO: In production, this would call the Control Plane API
        self.logger.info(
            "[PLACEHOLDER] Fetching connection config",
            connection_id=connection_id,
        )
        return {
            "connection_id": connection_id,
            "access_token": "placeholder_access_token",
            "refresh_token": "placeholder_refresh_token",
            "client_id": "placeholder_client_id",
            "client_secret": "placeholder_client_secret",
            "settings": {
                "sync_files": True,
                "sync_folders": True,
                "sync_shared_drives": True,
                "sync_permissions": False,
            },
        }

    def create_ingestion_envelope(
        self,
        message_id: str,
        customer_id: str,
        connection_id: str,
        resource_type: str,
        resource_id: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create an ingestion data envelope.

        This follows the schema defined in docs/04-message-schemas.md

        Args:
            message_id: Unique message ID (UUID v4)
            customer_id: Customer identifier
            connection_id: Integration connection ID
            resource_type: Type of resource
            resource_id: Resource identifier
            data: The normalized data
            metadata: Optional metadata

        Returns:
            Complete envelope for ingestion.data topic
        """
        now = datetime.utcnow().isoformat() + "Z"
        idempotency_key = self.generate_idempotency_key(
            connection_id,
            resource_type,
            resource_id,
        )

        return {
            "message_id": message_id,
            "customer_id": customer_id,
            "integration_id": self.integration_id,
            "integration_connection_id": connection_id,
            "provider": self.integration_id,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "data": data,
            "metadata": metadata or {},
            "created_at": now,
            "idempotency_key": idempotency_key,
        }

    async def handle_error(
        self,
        error: Exception,
        message: Dict[str, Any],
        connection_id: str,
    ) -> Dict[str, Any]:
        """Handle and format errors for the integration.errors topic.

        Args:
            error: The exception that occurred
            message: The original message being processed
            connection_id: The connection ID

        Returns:
            Error envelope for integration.errors topic
        """
        error_details = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "connection_id": connection_id,
            "original_message": message,
        }

        if isinstance(error, IntegrationError):
            error_details.update(error.to_dict())

        return {
            "message_id": message.get("message_id", "unknown"),
            "customer_id": message.get("customer_id", "unknown"),
            "integration_id": self.integration_id,
            "integration_connection_id": connection_id,
            "error": error_details,
            "occurred_at": datetime.utcnow().isoformat() + "Z",
        }