"""Init handler for Google Drive worker.

Validates Drive access and optionally sets up push notification webhooks.
Saves channel information to connection state for webhook processing and renewal.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from clustera_integration_toolkit.control_plane import ControlPlaneError
from clustera_integration_toolkit.message.message_dictionary import ConnectionReadyBuilder

from ..client.drive_api import GoogleDriveAPIClient
from ..config import GoogleDriveAPIConfig
from google_drive_worker.handlers.base import BaseGoogleDriveHandler
from google_drive_worker.utils.errors import ValidationError

# State keys for storing channel information
STATE_KEY_PAGE_TOKEN = "google_drive_page_token"
STATE_KEY_CHANNEL_ID = "google_drive_channel_id"
STATE_KEY_RESOURCE_ID = "google_drive_resource_id"
STATE_KEY_CHANNEL_EXPIRATION = "google_drive_channel_expiration"
STATE_KEY_CHANNEL_TOKEN = "google_drive_channel_token"


class InitHandler(BaseGoogleDriveHandler):
    """Handler for init action.

    Validates Google Drive API access and optionally sets up push notification
    webhooks. Returns operation results (profile info, watch status) and emits
    connection.ready event.
    """

    SUPPORTED_ACTIONS = {"init"}
    SUPPORTED_METHODS = {"clustera.integration.connection.initialize"}

    # Google Drive connection capabilities (Plan 17/41)
    CAPABILITIES = {
        "supports_backfill": True,
        "supports_incremental_sync": True,
        "supports_webhooks": True,
        "default_resource_type": "files",
        "supported_resource_types": [
            "files",    # List files with pagination
            "file",     # Single file by ID
            "folders",  # List folders with pagination
            "folder",   # Single folder by ID
        ],
        "max_results_per_page": 1000,
        "webhook_expiration_hours": 24,  # Google Drive max (vs Gmail 168 hours)
        # Plan 41: Hybrid integration (polling + webhooks)
        "requires_polling": True,  # Webhooks need daily renewal
    }

    def __init__(self, api_config: GoogleDriveAPIConfig) -> None:
        """Initialize init handler.

        Args:
            api_config: Google Drive API configuration
        """
        super().__init__(api_config)

    async def process_message(
        self,
        message: dict[str, Any],
        connection_config: dict[str, Any],
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Process an init message.

        Args:
            message: The init message
            connection_config: Connection configuration with credentials

        Yields:
            ConnectionReady event with initialization results
        """
        self._validate_message(message)

        # Extract connection info
        connection_id = message.get("integration_connection_id")
        customer_id = message.get("customer_id")
        trace_id = message.get("trace_id")

        # Parse init config
        config = message.get("config", {})
        setup_webhook = config.get("setup_webhook", True)
        webhook_url = config.get("webhook_url")  # Must be provided if setup_webhook=True

        self.logger.info(
            "Processing init action",
            connection_id=connection_id,
            setup_webhook=setup_webhook,
        )

        # Initialize API client
        api_client = GoogleDriveAPIClient(
            access_token=connection_config.get("access_token"),
            refresh_token=connection_config.get("refresh_token"),
            client_id=connection_config.get("client_id"),
            client_secret=connection_config.get("client_secret"),
        )

        api_calls = 0
        start_time = datetime.now(timezone.utc)

        try:
            # Step 1: Get Drive about info to validate access
            about = await api_client.get_about(fields="user,storageQuota")
            api_calls += 1

            user_info = about.get("user", {})
            email_address = user_info.get("emailAddress")

            self.logger.info(
                "Google Drive access validated",
                email_address=email_address,
            )

            watch_data = None

            # Step 2: Set up webhook if requested
            if setup_webhook:
                if not webhook_url:
                    self.logger.warning(
                        "Webhook setup requested but no webhook_url provided",
                        connection_id=connection_id,
                    )
                else:
                    # Get start page token for changes
                    page_token = await api_client.get_start_page_token(
                        supports_all_drives=True
                    )
                    api_calls += 1

                    # Create unique channel identifiers
                    channel_id = str(uuid.uuid4())
                    channel_token = str(uuid.uuid4())  # Verification token

                    # Set expiration to 23 hours (max is 24, leave buffer)
                    expiration_dt = datetime.now(timezone.utc) + timedelta(hours=23)
                    expiration_ms = int(expiration_dt.timestamp() * 1000)

                    # Create the notification channel
                    watch_response = await api_client.watch_changes(
                        page_token=page_token,
                        channel_id=channel_id,
                        webhook_url=webhook_url,
                        channel_token=channel_token,
                        expiration=expiration_ms,
                        include_shared_drives=True,
                    )
                    api_calls += 1

                    resource_id = watch_response.get("resourceId")
                    actual_expiration = watch_response.get("expiration")

                    self.logger.info(
                        "Google Drive watch channel created",
                        channel_id=channel_id,
                        resource_id=resource_id,
                        expiration=actual_expiration,
                    )

                    # Save channel information to connection state
                    if connection_id:
                        try:
                            # Use base handler's control_plane_client from toolkit
                            cp_client = self.control_plane_client

                            await cp_client.upsert_connection_state(
                                connection_id=connection_id,
                                key=STATE_KEY_PAGE_TOKEN,
                                value=page_token,
                            )
                            await cp_client.upsert_connection_state(
                                connection_id=connection_id,
                                key=STATE_KEY_CHANNEL_ID,
                                value=channel_id,
                            )
                            await cp_client.upsert_connection_state(
                                connection_id=connection_id,
                                key=STATE_KEY_RESOURCE_ID,
                                value=resource_id,
                            )
                            await cp_client.upsert_connection_state(
                                connection_id=connection_id,
                                key=STATE_KEY_CHANNEL_EXPIRATION,
                                value=str(actual_expiration),
                            )
                            await cp_client.upsert_connection_state(
                                connection_id=connection_id,
                                key=STATE_KEY_CHANNEL_TOKEN,
                                value=channel_token,
                            )

                            self.logger.info(
                                "Saved channel information to connection state",
                                connection_id=connection_id,
                                channel_id=channel_id,
                            )
                        except ControlPlaneError as e:
                            # Non-fatal: webhook will work but renewal may be harder
                            self.logger.warning(
                                "Failed to save channel state (non-fatal)",
                                error=str(e),
                                connection_id=connection_id,
                            )

                    watch_data = {
                        "channel_id": channel_id,
                        "resource_id": resource_id,
                        "expiration": actual_expiration,
                        "page_token": page_token,
                    }

            processing_duration = int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )

            self.logger.info(
                "Init operation completed successfully",
                connection_id=connection_id,
                api_calls_made=api_calls,
                processing_duration_ms=processing_duration,
                webhook_enabled=setup_webhook and watch_data is not None,
            )

            # Yield connection.ready event (Plan 17)
            profile_data = {}
            state_data = {}

            if email_address:
                profile_data["email_address"] = email_address
                profile_data["display_name"] = user_info.get("displayName")

            if watch_data:
                state_data["channel_id"] = watch_data.get("channel_id")
                state_data["channel_expiration"] = watch_data.get("expiration")
                state_data["page_token"] = watch_data.get("page_token")

            yield ConnectionReadyBuilder.from_connection(
                connection_config=connection_config,
                integration_provider_name="google-drive",
                capabilities=self.CAPABILITIES,
                profile=profile_data if profile_data else None,
                state=state_data if state_data else None,
                trace_id=trace_id,
            )

        finally:
            await api_client.close()

    def _validate_message(self, message: dict[str, Any]) -> None:
        """Validate required fields in init message.

        Supports both JSON-RPC 2.0 and legacy formats.

        Args:
            message: The message to validate

        Raises:
            ValidationError: If required fields are missing
        """
        # JSON-RPC 2.0 format validation
        if message.get("jsonrpc") == "2.0":
            if message.get("method") != "clustera.integration.connection.initialize":
                raise ValidationError(
                    f"Invalid method for InitHandler: {message.get('method')}",
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

        if message.get("action") != "init":
            raise ValidationError(
                f"Invalid action for InitHandler: {message.get('action')}",
                field="action",
            )
