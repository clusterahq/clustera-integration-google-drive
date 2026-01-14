"""Handler for Google Drive trigger-based (polling) integration.

Processes trigger messages from the integration.trigger topic and
fetches data from Google Drive API based on the trigger type.
"""

from typing import Any, Dict, AsyncIterator, Optional
import uuid
from datetime import datetime
import structlog

from .base import BaseIntegrationHandler
from ..client.drive_api import GoogleDriveAPIClient
from ..config import GoogleDriveAPIConfig
from ..utils.errors import ValidationError, RetriableError, TerminalError
from clustera_integration_toolkit.control_plane import ControlPlaneClient, ControlPlaneError

# State key for storing the last processed page token
STATE_KEY_PAGE_TOKEN = "google_drive_page_token"


class GoogleDriveTriggerHandler(BaseIntegrationHandler):
    """Handle polling triggers for Google Drive integration."""

    def __init__(
        self,
        api_config: GoogleDriveAPIConfig,
        logger: Optional[structlog.BoundLogger] = None,
    ):
        """Initialize the trigger handler.

        Args:
            api_config: Google Drive API configuration
            logger: Structured logger instance
        """
        super().__init__(integration_id="google-drive", logger=logger)
        self.api_config = api_config

    async def can_handle(self, message: Dict[str, Any]) -> bool:
        """Check if this handler can process the message.

        Args:
            message: The Kafka message value

        Returns:
            True if this is a Google Drive trigger message
        """
        integration_id = message.get("integration_id")
        return integration_id == "google-drive"

    async def process_message(
        self,
        message: Dict[str, Any],
        connection_config: Dict[str, Any],
    ) -> AsyncIterator[Dict[str, Any]]:
        """Process a trigger message and yield normalized records.

        Args:
            message: The trigger message from integration.trigger topic
            connection_config: Connection configuration with credentials

        Yields:
            Normalized file/folder records for ingestion.data topic

        Raises:
            ValidationError: If message is invalid
            RetriableError: For temporary failures
        """
        # Validate trigger message
        self._validate_trigger_message(message)

        trigger_type = message["trigger_type"]
        connection_id = message["integration_connection_id"]
        customer_id = message["customer_id"]

        self.logger.info(
            "Processing Google Drive trigger",
            trigger_type=trigger_type,
            connection_id=connection_id,
            customer_id=customer_id,
        )

        # Initialize API client
        api_client = GoogleDriveAPIClient(
            access_token=connection_config.get("access_token"),
            refresh_token=connection_config.get("refresh_token"),
            client_id=connection_config.get("client_id"),
            client_secret=connection_config.get("client_secret"),
        )

        try:
            if trigger_type == "full_sync":
                async for record in self._process_full_sync(
                    api_client, connection_id, customer_id, connection_config, message
                ):
                    yield record
            elif trigger_type in ["incremental", "poll"]:
                async for record in self._process_incremental(
                    api_client, connection_id, customer_id, connection_config, message
                ):
                    yield record
            else:
                raise ValidationError(f"Unknown trigger type: {trigger_type}")
        finally:
            await api_client.close()

    async def _process_full_sync(
        self,
        api_client: GoogleDriveAPIClient,
        connection_id: str,
        customer_id: str,
        connection_config: Dict[str, Any],
        message: Dict[str, Any],
    ) -> AsyncIterator[Dict[str, Any]]:
        """Process a full sync trigger.

        Args:
            api_client: Google Drive API client
            connection_id: Integration connection ID
            customer_id: Customer ID
            connection_config: Connection configuration
            message: Original trigger message

        Yields:
            All files/folders in the drive
        """
        self.logger.info(
            "Processing full sync",
            connection_id=connection_id,
            customer_id=customer_id,
        )

        # Get sync settings from connection config
        settings = connection_config.get("settings", {})
        sync_files = settings.get("sync_files", True)
        sync_folders = settings.get("sync_folders", True)
        sync_shared_drives = settings.get("sync_shared_drives", True)

        # Build query based on settings
        query_parts = []
        if not sync_files:
            query_parts.append("mimeType = 'application/vnd.google-apps.folder'")
        elif not sync_folders:
            query_parts.append("mimeType != 'application/vnd.google-apps.folder'")

        query = " and ".join(query_parts) if query_parts else None

        # Track progress
        total_files = 0
        page_token = None

        while True:
            try:
                # List files with pagination
                response = await api_client.list_files(
                    page_size=100,
                    page_token=page_token,
                    query=query,
                    include_shared_drives=sync_shared_drives,
                    include_trashed=False,
                )

                files = response.get("files", [])
                page_token = response.get("nextPageToken")

                # Process each file
                for file_data in files:
                    total_files += 1

                    # Skip if file type not wanted
                    is_folder = file_data.get("mimeType") == "application/vnd.google-apps.folder"
                    if is_folder and not sync_folders:
                        continue
                    if not is_folder and not sync_files:
                        continue

                    # Fetch content for Google Workspace files
                    file_content = None
                    if not is_folder:
                        file_content = await self._fetch_file_content(
                            api_client=api_client,
                            file_data=file_data,
                            connection_id=connection_id,
                        )

                    # Create normalized envelope
                    resource_type = "folder" if is_folder else "file"

                    yield await self.create_ingestion_envelope(
                        message_id=str(uuid.uuid4()),
                        customer_id=customer_id,
                        connection_id=connection_id,
                        resource_type=resource_type,
                        resource_id=file_data["id"],
                        data=self._normalize_file(file_data),
                        content=file_content,
                        metadata={
                            "trigger_type": "full_sync",
                            "source_format": "google_drive_api_v3",
                            "transformation_version": "1.0.0",
                            "has_content": file_content is not None,
                            "content_size_bytes": len(file_content) if file_content else 0,
                        },
                    )

                # Log progress
                if total_files % 100 == 0:
                    self.logger.info(
                        "Full sync progress",
                        connection_id=connection_id,
                        files_processed=total_files,
                        has_more=bool(page_token),
                    )

                # Check if more pages
                if not page_token:
                    break

            except Exception as e:
                self.logger.error(
                    "Error during full sync",
                    connection_id=connection_id,
                    error=str(e),
                    files_processed=total_files,
                )
                raise

        self.logger.info(
            "Full sync completed",
            connection_id=connection_id,
            total_files_processed=total_files,
        )

    async def _process_incremental(
        self,
        api_client: GoogleDriveAPIClient,
        connection_id: str,
        customer_id: str,
        connection_config: Dict[str, Any],
        message: Dict[str, Any],
    ) -> AsyncIterator[Dict[str, Any]]:
        """Process an incremental sync trigger.

        Args:
            api_client: Google Drive API client
            connection_id: Integration connection ID
            customer_id: Customer ID
            connection_config: Connection configuration
            message: The trigger message

        Yields:
            Changed files/folders since last sync
        """
        # Try to get page token from connection state first
        page_token = None
        try:
            # Initialize Control Plane client
            async with ControlPlaneClient() as cp_client:
                state_value = await cp_client.get_connection_state(
                    connection_id=connection_id,
                    key=STATE_KEY_PAGE_TOKEN,
                )
                if state_value:
                    page_token = state_value
                    self.logger.info(
                        "Retrieved page token from connection state",
                        connection_id=connection_id,
                        page_token_preview=page_token[:20] + "..." if len(page_token) > 20 else page_token,
                    )
        except Exception as e:
            self.logger.debug(
                "Could not retrieve page token from state (will fetch start token)",
                connection_id=connection_id,
                error=str(e),
            )
            page_token = None

        self.logger.info(
            "Processing incremental sync",
            connection_id=connection_id,
            has_page_token=bool(page_token),
        )

        # Get sync settings
        settings = connection_config.get("settings", {})
        sync_files = settings.get("sync_files", True)
        sync_folders = settings.get("sync_folders", True)
        sync_shared_drives = settings.get("sync_shared_drives", True)

        # If no page token, get start token
        if not page_token:
            self.logger.info(
                "No page token found, fetching start page token",
                connection_id=connection_id,
            )
            page_token = await api_client.get_start_page_token(
                supports_all_drives=sync_shared_drives
            )
            self.logger.info(
                "Retrieved start page token",
                connection_id=connection_id,
                page_token=page_token[:20] + "..." if len(page_token) > 20 else page_token,
            )

        # Track progress
        total_changes = 0
        new_start_page_token = None

        while page_token:
            try:
                # List changes
                response = await api_client.list_changes(
                    page_token=page_token,
                    page_size=100,
                    include_shared_drives=sync_shared_drives,
                )

                changes = response.get("changes", [])
                page_token = response.get("nextPageToken")
                new_start_page_token = response.get("newStartPageToken")

                # Process each change
                for change in changes:
                    total_changes += 1

                    # Skip removed files
                    if change.get("removed"):
                        self.logger.debug(
                            "Skipping removed file",
                            file_id=change.get("file", {}).get("id"),
                        )
                        continue

                    file_data = change.get("file")
                    if not file_data:
                        continue

                    # Check if file type is wanted
                    is_folder = file_data.get("mimeType") == "application/vnd.google-apps.folder"
                    if is_folder and not sync_folders:
                        continue
                    if not is_folder and not sync_files:
                        continue

                    # Fetch content for Google Workspace files
                    file_content = None
                    if not is_folder:
                        file_content = await self._fetch_file_content(
                            api_client=api_client,
                            file_data=file_data,
                            connection_id=connection_id,
                        )

                    # Create normalized envelope
                    resource_type = "folder" if is_folder else "file"

                    yield await self.create_ingestion_envelope(
                        message_id=str(uuid.uuid4()),
                        customer_id=customer_id,
                        connection_id=connection_id,
                        resource_type=resource_type,
                        resource_id=file_data["id"],
                        data=self._normalize_file(file_data),
                        content=file_content,
                        metadata={
                            "trigger_type": "incremental",
                            "change_type": change.get("changeType", "file"),
                            "source_format": "google_drive_api_v3",
                            "transformation_version": "1.0.0",
                            "has_content": file_content is not None,
                            "content_size_bytes": len(file_content) if file_content else 0,
                        },
                    )

                # Log progress
                if total_changes % 100 == 0:
                    self.logger.info(
                        "Incremental sync progress",
                        connection_id=connection_id,
                        changes_processed=total_changes,
                        has_more=bool(page_token),
                    )

            except Exception as e:
                self.logger.error(
                    "Error during incremental sync",
                    connection_id=connection_id,
                    error=str(e),
                    changes_processed=total_changes,
                )
                raise

        # Log the new start page token for next incremental sync
        if new_start_page_token:
            self.logger.info(
                "Incremental sync completed - page token will be saved to connection state",
                connection_id=connection_id,
                new_start_page_token=new_start_page_token[:20] + "..." if len(new_start_page_token) > 20 else new_start_page_token,
                total_changes_processed=total_changes,
            )

            # Save the new page token to connection state for next incremental sync
            try:
                async with ControlPlaneClient() as cp_client:
                    await cp_client.upsert_connection_state(
                        connection_id=connection_id,
                        key=STATE_KEY_PAGE_TOKEN,
                        value=new_start_page_token,
                    )
                self.logger.info(
                    "Saved page token to connection state for next sync",
                    connection_id=connection_id,
                    page_token_preview=new_start_page_token[:20] + "..." if len(new_start_page_token) > 20 else new_start_page_token,
                )
            except ControlPlaneError as e:
                # Non-fatal: next sync will restart from beginning
                self.logger.warning(
                    "Failed to save page token to connection state (non-fatal)",
                    connection_id=connection_id,
                    error=str(e),
                )
        else:
            self.logger.info(
                "Incremental sync completed",
                connection_id=connection_id,
                total_changes_processed=total_changes,
            )

    async def _fetch_file_content(
        self,
        api_client: GoogleDriveAPIClient,
        file_data: Dict[str, Any],
        connection_id: str,
    ) -> Optional[bytes]:
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

    def _normalize_file(self, raw_file: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Google Drive file to standard format.

        Args:
            raw_file: Raw file data from API

        Returns:
            Normalized file data
        """
        # Basic normalization - full transformation will be done in Phase 4
        normalized = {
            "id": raw_file.get("id"),
            "name": raw_file.get("name"),
            "mime_type": raw_file.get("mimeType"),
            "created_at": raw_file.get("createdTime"),
            "modified_at": raw_file.get("modifiedTime"),
            "size_bytes": int(raw_file.get("size", 0)) if raw_file.get("size") else None,
            "web_view_link": raw_file.get("webViewLink"),
            "parents": raw_file.get("parents", []),
            "trashed": raw_file.get("trashed", False),
        }

        # Determine type
        if raw_file.get("mimeType") == "application/vnd.google-apps.folder":
            normalized["type"] = "folder"
        else:
            normalized["type"] = "file"

        # Add owner info if present
        owners = raw_file.get("owners", [])
        if owners:
            normalized["owner"] = {
                "email": owners[0].get("emailAddress"),
                "name": owners[0].get("displayName"),
            }

        # Add permissions count if present
        permissions = raw_file.get("permissions", [])
        if permissions:
            normalized["permissions_count"] = len(permissions)

        return normalized

    def generate_idempotency_key(
        self,
        connection_id: str,
        resource_type: str,
        resource_id: str,
    ) -> str:
        """Generate deterministic idempotency key.

        Args:
            connection_id: Integration connection ID
            resource_type: Type of resource (file, folder, etc.)
            resource_id: Unique resource ID

        Returns:
            Idempotency key following pattern
        """
        return f"google-drive:{connection_id}:{resource_type}:{resource_id}"

    def _validate_trigger_message(self, message: Dict[str, Any]) -> None:
        """Validate required fields in trigger message.

        Args:
            message: The trigger message

        Raises:
            ValidationError: If required fields are missing
        """
        required_fields = [
            "message_id",
            "customer_id",
            "integration_id",
            "integration_connection_id",
            "trigger_type",
        ]

        missing_fields = []
        for field in required_fields:
            if field not in message or message[field] is None:
                missing_fields.append(field)

        if missing_fields:
            raise ValidationError(f"Missing required fields: {', '.join(missing_fields)}")

        # Validate trigger type
        valid_trigger_types = ["full_sync", "incremental", "poll"]
        if message["trigger_type"] not in valid_trigger_types:
            raise ValidationError(
                f"Invalid trigger_type: {message['trigger_type']}. "
                f"Must be one of: {', '.join(valid_trigger_types)}"
            )