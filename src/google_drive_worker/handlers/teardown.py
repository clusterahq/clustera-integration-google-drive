"""Teardown handler for Google Drive worker.

Stops push notification webhooks and cleans up connection state.
Per Plan 37, teardown is silent - no messages emitted to integrations-incoming-records.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from clustera_integration_toolkit.control_plane import ControlPlaneClient, ControlPlaneError

from .base import BaseIntegrationHandler
from ..client.drive_api import GoogleDriveAPIClient
from ..utils.errors import ValidationError

# State keys (must match init.py when it's created)
STATE_KEY_CHANNEL_ID = "google_drive_channel_id"
STATE_KEY_RESOURCE_ID = "google_drive_resource_id"


class TeardownHandler(BaseIntegrationHandler):
    """Handler for teardown action.

    Stops Google Drive push notification webhooks and cleans up resources.
    Returns no output (silent teardown per Plan 37).
    """

    SUPPORTED_ACTIONS = {"teardown"}
    SUPPORTED_METHODS = {"clustera.integration.connection.teardown"}

    def __init__(self, integration_id: str = "google-drive", logger: Optional[Any] = None) -> None:
        """Initialize teardown handler.

        Args:
            integration_id: The integration identifier
            logger: Optional structured logger instance
        """
        super().__init__(integration_id=integration_id, logger=logger)

    async def can_handle(self, message: Dict[str, Any]) -> bool:
        """Check if this handler can process the message.

        Args:
            message: The Kafka message value

        Returns:
            True if this is a teardown message for Google Drive
        """
        # Check JSON-RPC 2.0 format
        if message.get("method") in self.SUPPORTED_METHODS:
            return True

        # Check legacy format
        if message.get("action") in self.SUPPORTED_ACTIONS:
            integration_id = message.get("integration_id")
            return integration_id == "google-drive"

        return False

    async def process_message(
        self,
        message: Dict[str, Any],
        connection_config: Dict[str, Any],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Process a teardown message.

        Per Plan 37, teardown is silent - no messages are emitted to
        integrations-incoming-records. The handler logs success for observability.

        Args:
            message: The teardown message
            connection_config: Connection configuration with credentials

        Yields:
            Nothing - teardown is silent per Plan 37
        """
        self._validate_message(message)

        # Extract connection info - support both JSON-RPC and legacy formats
        if message.get("jsonrpc") == "2.0":
            params = message.get("params", {})
            header = params.get("header", {})
            header_params = header.get("parameters", {})
            connection_id = header_params.get("integration_connection_id")
            customer_id = header_params.get("customer_id")
        else:
            # Legacy format
            connection_id = message.get("integration_connection_id")
            customer_id = message.get("customer_id")

        self.logger.info(
            "Processing teardown action",
            connection_id=connection_id,
        )

        # Initialize API client
        api_client = GoogleDriveAPIClient(
            access_token=connection_config.get("access_token", ""),
            refresh_token=connection_config.get("refresh_token", ""),
            client_id=connection_config.get("client_id", ""),
            client_secret=connection_config.get("client_secret", ""),
        )

        api_calls = 0
        start_time = datetime.now(timezone.utc)
        channel_stopped = False
        stop_error = None

        try:
            # Step 1: Stop the notification channel
            try:
                # Retrieve channel info from connection state
                async with ControlPlaneClient() as cp_client:
                    channel_id = await cp_client.get_connection_state(
                        connection_id=connection_id,
                        key=STATE_KEY_CHANNEL_ID,
                    )

                    resource_id = await cp_client.get_connection_state(
                        connection_id=connection_id,
                        key=STATE_KEY_RESOURCE_ID,
                    )

                # Stop the channel if we have the info
                if channel_id and resource_id:
                    await api_client.stop_channel(
                        channel_id=channel_id,
                        resource_id=resource_id,
                    )
                    api_calls += 1
                    channel_stopped = True

                    self.logger.info(
                        "Google Drive watch channel stopped",
                        channel_id=channel_id,
                    )
                else:
                    self.logger.info(
                        "No channel info found in state (may not have been set up)",
                        connection_id=connection_id,
                    )

            except Exception as e:
                # Log but don't fail - the channel may have already expired
                self.logger.warning(
                    "Failed to stop Google Drive watch channel",
                    error=str(e),
                    connection_id=connection_id,
                )
                stop_error = str(e)

            # Step 2: Clean up all connection state
            state_cleaned = False
            try:
                async with ControlPlaneClient() as cp_client:
                    await cp_client.delete_all_connection_state(
                        connection_id=connection_id
                    )
                state_cleaned = True

                self.logger.info(
                    "Connection state cleaned up",
                    connection_id=connection_id,
                )
            except ControlPlaneError as e:
                # Non-fatal: state will be orphaned but won't cause issues
                self.logger.warning(
                    "Failed to clean up connection state (non-fatal)",
                    error=str(e),
                    connection_id=connection_id,
                )

            processing_duration = int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )

            # Per Plan 37: Teardown is silent - no response message to incoming-records
            # Log success for observability
            self.logger.info(
                "Teardown completed successfully",
                connection_id=connection_id,
                customer_id=customer_id,
                api_calls_made=api_calls,
                processing_duration_ms=processing_duration,
                channel_stopped=channel_stopped,
                state_cleaned=state_cleaned,
                stop_error=stop_error,
            )

            # No message emitted to integrations-incoming-records
            # Empty generator - yield nothing but maintain async generator signature
            return
            yield  # pragma: no cover - makes this an async generator

        finally:
            await api_client.close()

    def _validate_message(self, message: Dict[str, Any]) -> None:
        """Validate required fields in teardown message.

        Supports both JSON-RPC 2.0 and legacy formats.

        Args:
            message: The message to validate

        Raises:
            ValidationError: If required fields are missing
        """
        # JSON-RPC 2.0 format validation
        if message.get("jsonrpc") == "2.0":
            if message.get("method") != "clustera.integration.connection.teardown":
                raise ValidationError(
                    f"Invalid method for TeardownHandler: {message.get('method')}",
                    field="method",
                )

            params = message.get("params", {})
            header = params.get("header", {})
            header_params = header.get("parameters", {})

            if not header_params.get("integration_connection_id"):
                raise ValidationError(
                    "Missing required field: params.header.parameters.integration_connection_id",
                    field="integration_connection_id",
                )
            return

        # Legacy format validation
        required_fields = [
            "message_id",
            "customer_id",
            "integration_id",
            "integration_connection_id",
            "action",
        ]

        for field in required_fields:
            if field not in message:
                raise ValidationError(
                    f"Missing required field: {field}",
                    field=field,
                )

        if message.get("action") != "teardown":
            raise ValidationError(
                f"Invalid action for TeardownHandler: {message.get('action')}",
                field="action",
            )

    def generate_idempotency_key(
        self,
        connection_id: str,
        resource_type: str,
        resource_id: str,
    ) -> str:
        """Generate deterministic idempotency key.

        Pattern: google-drive:{connection_id}:{resource_type}:{resource_id}

        Args:
            connection_id: Integration connection ID
            resource_type: Type of resource (file, folder, etc.)
            resource_id: Unique resource ID

        Returns:
            Idempotency key following pattern
        """
        return f"google-drive:{connection_id}:{resource_type}:{resource_id}"
