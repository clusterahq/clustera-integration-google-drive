"""Handler for Google Drive webhook (push notification) integration.

Processes webhook messages from the webhook.raw topic when Google Drive
sends push notifications about file changes.
"""

from typing import Any, Dict, AsyncIterator, Optional
import uuid
from urllib.parse import urlparse, parse_qs
import structlog

from .base import BaseIntegrationHandler
from ..config import GoogleDriveAPIConfig
from ..utils.errors import ValidationError


class GoogleDriveWebhookHandler(BaseIntegrationHandler):
    """Handle push notifications from Google Drive."""

    def __init__(
        self,
        api_config: GoogleDriveAPIConfig,
        logger: Optional[structlog.BoundLogger] = None,
    ):
        """Initialize the webhook handler.

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
            True if this is a Google Drive webhook message
        """
        provider = message.get("provider")
        integration_id = message.get("integration_id")
        return provider == "google-drive" or integration_id == "google-drive"

    async def process_message(
        self,
        message: Dict[str, Any],
        connection_config: Dict[str, Any],
    ) -> AsyncIterator[Dict[str, Any]]:
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
        """
        connection_id = message["integration_connection_id"]
        customer_id = message["customer_id"]
        payload = message.get("payload", {})

        self.logger.info(
            "Processing Google Drive webhook",
            connection_id=connection_id,
            channel_id=payload.get("id"),
        )

        # Extract resource URI which contains the page token
        resource_uri = payload.get("resourceUri", "")
        page_token = self._extract_page_token(resource_uri)

        if not page_token:
            self.logger.warning(
                "No page token in webhook notification",
                connection_id=connection_id,
            )
            return

        # TODO: In Phase 5, implement actual Changes API call
        # For now, yield placeholder changed file
        async for record in self._fetch_changes(
            connection_id, customer_id, page_token, connection_config
        ):
            yield record

    async def _fetch_changes(
        self,
        connection_id: str,
        customer_id: str,
        page_token: str,
        connection_config: Dict[str, Any],
    ) -> AsyncIterator[Dict[str, Any]]:
        """Fetch changes from Google Drive using the Changes API.

        Args:
            connection_id: Integration connection ID
            customer_id: Customer ID
            page_token: Page token from webhook notification
            connection_config: Connection configuration

        Yields:
            Changed files
        """
        self.logger.info(
            "[PLACEHOLDER] Fetching changes from webhook notification",
            connection_id=connection_id,
            page_token=page_token,
        )

        # TODO: Implement actual Changes API call
        # Placeholder: yield sample changed file
        changed_file = {
            "id": "file_789",
            "name": "Webhook Updated File.pdf",
            "mimeType": "application/pdf",
            "modifiedTime": "2025-01-20T12:00:00Z",
        }

        yield self.create_ingestion_envelope(
            message_id=str(uuid.uuid4()),
            customer_id=customer_id,
            connection_id=connection_id,
            resource_type="file",
            resource_id=changed_file["id"],
            data=self._normalize_file(changed_file),
            metadata={
                "source": "webhook",
                "page_token": page_token,
            },
        )

    def _normalize_file(self, raw_file: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Google Drive file to standard format.

        Args:
            raw_file: Raw file data from API

        Returns:
            Normalized file data
        """
        # TODO: Share normalization logic with trigger handler
        return {
            "id": raw_file.get("id"),
            "type": "file",
            "name": raw_file.get("name"),
            "mime_type": raw_file.get("mimeType"),
            "modified_at": raw_file.get("modifiedTime"),
        }

    def _extract_page_token(self, resource_uri: str) -> Optional[str]:
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
                "Failed to extract page token",
                resource_uri=resource_uri,
                error=str(e),
            )
            return None

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