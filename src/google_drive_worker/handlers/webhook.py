"""Handler for Google Drive webhook (push notification) integration.

Processes webhook messages from the webhook.raw topic when Google Drive
sends push notifications about file changes.

Per Phase 5 (Webhook External ID Resolution):
- Extracts emailAddress from Google Drive push notification
- Resolves connection by external_id (email) using Control Plane
- Caches resolutions for 5-minute TTL
"""

from __future__ import annotations

from typing import Any
import uuid
from urllib.parse import urlparse, parse_qs
from collections.abc import AsyncGenerator

from clustera_integration_toolkit.control_plane import (
    ConnectionResolver,
    ControlPlaneNotFoundError,
    ResolvedConnection,
)

from google_drive_worker.client.drive_api import GoogleDriveAPIClient
from google_drive_worker.config import GoogleDriveAPIConfig, StorageConfig
from google_drive_worker.handlers.base import BaseActionHandler
from google_drive_worker.handlers.content_emitter import ContentIngestEmitter
from google_drive_worker.normalization.transformer import GoogleDriveDataTransformer
from google_drive_worker.utils.errors import ValidationError, RetriableError, TerminalError


class WebhookHandler(BaseActionHandler):
    """Handle push notifications from Google Drive.

    Per Phase 5 (Webhook External ID Resolution):
    - Extracts emailAddress from Google Drive push notification
    - Resolves connection by external_id (email) using Control Plane
    - Caches resolutions for 5-minute TTL
    """

    SUPPORTED_ACTIONS = {"webhook"}
    SUPPORTED_METHODS = {"clustera.integration.webhook"}

    def __init__(
        self,
        api_config: GoogleDriveAPIConfig,
        storage_config: StorageConfig | None = None,
        provider_name: str | None = None,
    ):
        """Initialize the webhook handler.

        Args:
            api_config: Google Drive API configuration
            storage_config: Storage configuration
            provider_name: Provider name for external ID resolution (e.g., "google-drive")
        """
        super().__init__(api_config)
        self.storage_config = storage_config or StorageConfig()
        self.provider_name = provider_name
        self.transformer = GoogleDriveDataTransformer()
        self._content_emitter: ContentIngestEmitter | None = None
        self._connection_resolver: ConnectionResolver | None = None

    @property
    def content_emitter(self) -> ContentIngestEmitter:
        """Lazy-initialize content ingest emitter."""
        if self._content_emitter is None:
            self._content_emitter = ContentIngestEmitter(
                api_config=self.api_config,
                storage_config=self.storage_config,
                logger=self.logger,
            )
        return self._content_emitter

    @property
    def connection_resolver(self) -> ConnectionResolver | None:
        """Lazy-initialize connection resolver for external ID lookups.

        Returns None if provider_name was not provided during initialization.
        """
        if self.provider_name is None:
            return None

        if self._connection_resolver is None:
            self._connection_resolver = ConnectionResolver(
                provider_name=self.provider_name,
                external_id_extractor=self._extract_email_from_notification,
                cache_ttl=300,  # 5 minutes
                cache_maxsize=1000,
                logger=self.logger,
            )
        return self._connection_resolver

    def _extract_email_from_notification(self, message: dict[str, Any]) -> str:
        """Extract email address from Google Drive push notification.

        Google Drive push notifications include the user's email in the payload.

        Args:
            message: The webhook message

        Returns:
            Email address from notification

        Raises:
            ValueError: If email address cannot be extracted
        """
        # Check JSON-RPC 2.0 format first
        if message.get("jsonrpc") == "2.0":
            params = message.get("params", {})
            payload = params.get("payload", {})
            email = payload.get("emailAddress")
            if email:
                return email

        # Legacy format
        payload = message.get("payload", {})
        email = payload.get("emailAddress")
        if email:
            return email

        raise ValueError("No emailAddress found in notification payload")

    async def process_message(
        self,
        message: dict[str, Any],
        connection_config: dict[str, Any],
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Process a webhook notification and yield normalized records.

        Google Drive push notifications don't contain the actual changes,
        just a notification that changes occurred. We need to use the
        Changes API to fetch the actual changes.

        Args:
            message: The webhook message from webhook.raw topic
            connection_config: Connection configuration with credentials

        Yields:
            Normalized file records for changed files

        Raises:
            ValidationError: If message is invalid
            RetriableError: For temporary failures
        """
        # Validate webhook message
        self._validate_webhook_message(message)

        connection_id = message["integration_connection_id"]
        customer_id = message["customer_id"]
        payload = message.get("payload", {})

        # Check if connection needs to be resolved via external ID
        needs_resolution = connection_id in ("unknown", "pending_lookup", "")
        resolved_connection: ResolvedConnection | None = None

        if needs_resolution and self.connection_resolver is not None:
            email_address = self._extract_email_from_notification(message)

            self.logger.info(
                "Connection ID unavailable, attempting external ID resolution",
                email_address=email_address,
                provider_name=self.provider_name,
            )
            try:
                resolved_connection = await self.connection_resolver.resolve(
                    message=message,
                    connection_id=None,  # Force external ID lookup
                )
                # Update values from resolved connection
                connection_id = resolved_connection.connection_id
                customer_id = resolved_connection.customer_id

                # Merge resolved credentials into connection_config
                connection_config = {
                    **connection_config,
                    "integration_connection_id": resolved_connection.connection_id,
                    "customer_id": resolved_connection.customer_id,
                    "snowball_id": resolved_connection.snowball_id,
                    "access_token": resolved_connection.credentials.access_token,
                    "refresh_token": resolved_connection.credentials.refresh_token,
                }

                self.logger.info(
                    "Connection resolved via external ID",
                    connection_id=connection_id,
                    customer_id=customer_id,
                    resolved_via=resolved_connection.resolved_via,
                    email_address=email_address,
                )

            except ControlPlaneNotFoundError as e:
                self.logger.error(
                    "Failed to resolve connection by external ID",
                    email_address=email_address,
                    error=str(e),
                )
                raise TerminalError(
                    f"No connection found for email: {email_address}",
                    category="connection_resolution",
                    details={
                        "error_code": "CONNECTION_NOT_FOUND",
                        "email_address": email_address,
                    },
                ) from e

            except ValueError as e:
                # Could not extract external ID from message
                self.logger.error(
                    "Failed to extract external ID from webhook message",
                    error=str(e),
                )
                raise TerminalError(
                    f"Cannot extract email from webhook message: {e}",
                    category="connection_resolution",
                    details={
                        "error_code": "EXTERNAL_ID_EXTRACTION_FAILED",
                    },
                ) from e

        self.logger.info(
            "Processing Google Drive webhook",
            connection_id=connection_id,
            customer_id=customer_id,
            channel_id=payload.get("id"),
            resource_id=payload.get("resourceId"),
        )

        # Check for channel stop notification (cleanup)
        if payload.get("kind") == "drive#stop":
            self.logger.info(
                "Received channel stop notification",
                connection_id=connection_id,
                channel_id=payload.get("id"),
            )
            return  # No data to process for stop notifications

        # Extract resource URI which contains the page token
        resource_uri = payload.get("resourceUri", "")
        page_token = self._extract_page_token(resource_uri)

        if not page_token:
            self.logger.warning(
                "No page token in webhook notification",
                connection_id=connection_id,
                resource_uri=resource_uri,
            )
            # This might be an initial setup notification, skip it
            return

        # Initialize API client
        api_client = GoogleDriveAPIClient(
            access_token=connection_config.get("access_token"),
            refresh_token=connection_config.get("refresh_token"),
            client_id=connection_config.get("client_id"),
            client_secret=connection_config.get("client_secret"),
        )

        try:
            # Fetch changes using the page token from the notification
            async for record in self._fetch_changes(
                api_client=api_client,
                connection_id=connection_id,
                customer_id=customer_id,
                page_token=page_token,
                connection_config=connection_config,
                message=message,
            ):
                yield record
        finally:
            await api_client.close()

    async def _fetch_file_content(
        self,
        api_client: GoogleDriveAPIClient,
        file_data: dict[str, Any],
        connection_id: str,
    ) -> bytes | None:
        """Fetch file content for Google Workspace files that need export.

        Args:
            api_client: Google Drive API client
            file_data: File metadata from API
            connection_id: Integration connection ID

        Returns:
            Exported file content as bytes, or None if not exportable
        """
        from ..normalization.mime_types import needs_export, get_export_format

        mime_type = file_data.get("mimeType")
        file_id = file_data.get("id")

        # Check if this file needs export (Google Workspace files)
        if not needs_export(mime_type):
            return None

        # Get the export format
        export_format = get_export_format(mime_type)
        if not export_format:
            self.logger.warning(
                "File needs export but no export format found",
                file_id=file_id,
                mime_type=mime_type,
                connection_id=connection_id,
            )
            return None

        try:
            self.logger.info(
                "Exporting Google Workspace file",
                file_id=file_id,
                mime_type=mime_type,
                export_format=export_format,
                connection_id=connection_id,
            )

            # Export the file using the API
            content = await api_client.export_file(
                file_id=file_id,
                mime_type=export_format,
            )

            self.logger.info(
                "Successfully exported file",
                file_id=file_id,
                content_size_bytes=len(content),
                connection_id=connection_id,
            )

            return content

        except Exception as e:
            self.logger.error(
                "Failed to export file",
                file_id=file_id,
                mime_type=mime_type,
                export_format=export_format,
                error=str(e),
                connection_id=connection_id,
            )
            # Non-fatal: we still have metadata
            return None

    async def _fetch_changes(
        self,
        api_client: GoogleDriveAPIClient,
        connection_id: str,
        customer_id: str,
        page_token: str,
        connection_config: dict[str, Any],
        message: dict[str, Any],
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Fetch changes from Google Drive using the Changes API.

        Args:
            api_client: Google Drive API client
            connection_id: Integration connection ID
            customer_id: Customer ID
            page_token: Page token from webhook notification
            connection_config: Connection configuration
            message: Original webhook message

        Yields:
            Normalized changed files
        """
        self.logger.info(
            "Fetching changes from webhook notification",
            connection_id=connection_id,
            page_token_prefix=page_token[:20] + "..." if len(page_token) > 20 else page_token,
        )

        # Get sync settings from connection config
        settings = connection_config.get("settings", {})
        sync_files = settings.get("sync_files", True)
        sync_folders = settings.get("sync_folders", True)
        sync_shared_drives = settings.get("sync_shared_drives", True)

        # Track progress
        total_changes = 0
        total_yielded = 0

        try:
            # Fetch changes using the page token
            response = await api_client.list_changes(
                page_token=page_token,
                page_size=100,
                include_shared_drives=sync_shared_drives,
            )

            changes = response.get("changes", [])
            next_page_token = response.get("nextPageToken")
            new_start_page_token = response.get("newStartPageToken")

            self.logger.info(
                "Retrieved changes from webhook",
                connection_id=connection_id,
                change_count=len(changes),
                has_more=bool(next_page_token),
            )

            # Process each change
            for change in changes:
                total_changes += 1

                # Skip removed files
                if change.get("removed"):
                    file_id = change.get("fileId")
                    self.logger.debug(
                        "Skipping removed file",
                        connection_id=connection_id,
                        file_id=file_id,
                    )
                    continue

                # Get file data from the change
                file_data = change.get("file")
                if not file_data:
                    self.logger.warning(
                        "Change has no file data",
                        connection_id=connection_id,
                        change_id=change.get("fileId"),
                    )
                    continue

                # Check if file type is wanted based on settings
                mime_type = file_data.get("mimeType", "")
                is_folder = mime_type == "application/vnd.google-apps.folder"

                if is_folder and not sync_folders:
                    self.logger.debug(
                        "Skipping folder based on settings",
                        connection_id=connection_id,
                        folder_id=file_data.get("id"),
                    )
                    continue

                if not is_folder and not sync_files:
                    self.logger.debug(
                        "Skipping file based on settings",
                        connection_id=connection_id,
                        file_id=file_data.get("id"),
                    )
                    continue

                # Fetch content for Google Workspace files
                file_content = None
                if not is_folder:
                    file_content = await self._fetch_file_content(
                        api_client=api_client,
                        file_data=file_data,
                        connection_id=connection_id,
                    )

                # Get snowball_id for content.ingest (required)
                snowball_id = connection_config.get("snowball_id")
                if not snowball_id:
                    self.logger.warning(
                        "Missing snowball_id in connection config, skipping file",
                        connection_id=connection_id,
                        file_id=file_data.get("id"),
                    )
                    continue

                # Use ContentIngestEmitter to build content.ingest message
                # This ensures consistent message format between webhook and backfill paths
                yield await self.content_emitter.process_file_for_ingest(
                    file_data=file_data,
                    connection_config=connection_config,
                    customer_id=customer_id,
                    snowball_id=snowball_id,
                    batch_context={
                        "batch_id": str(uuid.uuid4()),
                        "batch_sequence": total_yielded,
                        "batch_is_last": False,  # Webhooks don't track "last" message
                        "batch_page_token": page_token,
                        "source": "webhook",
                        "change_type": change.get("changeType", "file"),
                    },
                )
                total_yielded += 1

            # Log summary
            self.logger.info(
                "Webhook processing completed",
                connection_id=connection_id,
                total_changes=total_changes,
                total_yielded=total_yielded,
                skipped=total_changes - total_yielded,
                new_start_page_token=new_start_page_token[:20] + "..." if new_start_page_token and len(new_start_page_token) > 20 else new_start_page_token,
            )

            # Note: We typically don't paginate through all changes in webhook processing
            # as that could be a lot of data. The webhook is meant to notify about recent changes.
            # If there are more pages, they'll be picked up by the next webhook or incremental sync.
            if next_page_token:
                self.logger.info(
                    "Additional changes available but not fetching in webhook handler",
                    connection_id=connection_id,
                    next_page_token=next_page_token[:20] + "...",
                )

        except Exception as e:
            self.logger.error(
                "Error fetching changes from webhook",
                connection_id=connection_id,
                error=str(e),
                changes_processed=total_changes,
                yielded=total_yielded,
            )
            raise RetriableError(f"Failed to fetch changes: {e}")

    def _extract_page_token(self, resource_uri: str) -> str | None:
        """Extract page token from Google Drive resource URI.

        The resource URI looks like:
        https://www.googleapis.com/drive/v3/changes?pageToken=...

        Args:
            resource_uri: The resource URI from webhook

        Returns:
            Page token if found, None otherwise
        """
        if not resource_uri:
            return None

        try:
            parsed = urlparse(resource_uri)
            params = parse_qs(parsed.query)
            tokens = params.get("pageToken", [])
            return tokens[0] if tokens else None
        except Exception as e:
            self.logger.warning(
                "Failed to extract page token from resource URI",
                resource_uri=resource_uri,
                error=str(e),
            )
            return None

    def _validate_webhook_message(self, message: dict[str, Any]) -> None:
        """Validate required fields in webhook message.

        Args:
            message: The webhook message

        Raises:
            ValidationError: If required fields are missing
        """
        required_fields = [
            "message_id",
            "customer_id",
            "integration_connection_id",
            "payload",
        ]

        missing_fields = []
        for field in required_fields:
            if field not in message or message[field] is None:
                missing_fields.append(field)

        if missing_fields:
            raise ValidationError(f"Missing required fields in webhook message: {', '.join(missing_fields)}")

        # Validate payload structure
        payload = message.get("payload", {})
        if not isinstance(payload, dict):
            raise ValidationError("Webhook payload must be a dictionary")

        # Check for required Google Drive webhook fields
        if "id" not in payload:  # Channel ID
            raise ValidationError("Missing channel ID in webhook payload")

        # resourceUri is required for change notifications (not for stop notifications)
        if payload.get("kind") != "drive#stop" and "resourceUri" not in payload:
            raise ValidationError("Missing resourceUri in webhook payload")