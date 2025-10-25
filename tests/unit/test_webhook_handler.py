"""Unit tests for Google Drive webhook handler."""

import json
import uuid
from typing import Any, Dict
from unittest.mock import AsyncMock, Mock, patch

import pytest

from google_drive_worker.handlers.webhook import GoogleDriveWebhookHandler
from google_drive_worker.config import GoogleDriveAPIConfig
from google_drive_worker.utils.errors import ValidationError, RetriableError


@pytest.fixture
def api_config():
    """Create test API configuration."""
    return GoogleDriveAPIConfig(
        client_id="test_client_id",
        client_secret="test_client_secret",
        redirect_uri="http://localhost:8000/callback",
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )


@pytest.fixture
def handler(api_config):
    """Create webhook handler instance."""
    return GoogleDriveWebhookHandler(api_config=api_config)


@pytest.fixture
def mock_api_client():
    """Create mock API client."""
    client = AsyncMock()
    client.close = AsyncMock()
    client.list_changes = AsyncMock()
    return client


@pytest.fixture
def valid_webhook_message():
    """Create a valid webhook message."""
    return {
        "message_id": str(uuid.uuid4()),
        "customer_id": "cust_123",
        "integration_id": "google-drive",
        "integration_connection_id": "conn_abc123",
        "provider": "google-drive",
        "webhook_id": "channel_123",
        "received_at": "2025-10-24T12:00:00Z",
        "payload": {
            "kind": "drive#change",
            "id": "channel_id_123",
            "resourceId": "resource_id_xyz",
            "resourceUri": "https://www.googleapis.com/drive/v3/changes?pageToken=abc123",
            "expiration": "1234567890000",
        },
    }


@pytest.fixture
def connection_config():
    """Create test connection configuration."""
    return {
        "access_token": "test_access_token",
        "refresh_token": "test_refresh_token",
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "settings": {
            "sync_files": True,
            "sync_folders": True,
            "sync_shared_drives": True,
            "sync_permissions": False,
        },
    }


@pytest.fixture
def sample_file_data():
    """Create sample file data."""
    return {
        "id": "file_123",
        "name": "Document.docx",
        "mimeType": "application/vnd.google-apps.document",
        "createdTime": "2025-01-15T10:00:00Z",
        "modifiedTime": "2025-01-20T15:30:00Z",
        "size": "12345",
        "parents": ["folder_abc"],
        "owners": [
            {
                "displayName": "Alice Smith",
                "emailAddress": "alice@example.com",
            }
        ],
        "webViewLink": "https://drive.google.com/file/d/file_123/view",
        "thumbnailLink": "https://drive.google.com/thumbnail?id=file_123",
        "trashed": False,
        "starred": False,
    }


@pytest.fixture
def sample_folder_data():
    """Create sample folder data."""
    return {
        "id": "folder_456",
        "name": "Project Files",
        "mimeType": "application/vnd.google-apps.folder",
        "createdTime": "2025-01-10T08:00:00Z",
        "modifiedTime": "2025-01-22T14:20:00Z",
        "parents": ["root"],
        "owners": [
            {
                "displayName": "Bob Johnson",
                "emailAddress": "bob@example.com",
            }
        ],
        "webViewLink": "https://drive.google.com/drive/folders/folder_456",
        "trashed": False,
        "starred": True,
        "folderColorRgb": "#0f9d58",
    }


class TestCanHandle:
    """Test the can_handle method."""

    def test_can_handle_with_google_drive_provider(self, handler):
        """Test handler accepts messages with google-drive provider."""
        message = {"provider": "google-drive"}
        assert handler.can_handle(message) is True

    def test_can_handle_with_google_drive_integration_id(self, handler):
        """Test handler accepts messages with google-drive integration_id."""
        message = {"integration_id": "google-drive"}
        assert handler.can_handle(message) is True

    def test_can_handle_with_both_fields(self, handler):
        """Test handler accepts messages with both provider and integration_id."""
        message = {
            "provider": "google-drive",
            "integration_id": "google-drive",
        }
        assert handler.can_handle(message) is True

    @pytest.mark.parametrize("provider", ["circle", "github", "slack", "notion"])
    def test_rejects_other_providers(self, handler, provider):
        """Test handler rejects other providers."""
        message = {"provider": provider}
        assert handler.can_handle(message) is False

    def test_rejects_missing_fields(self, handler):
        """Test handler rejects message without provider or integration_id."""
        message = {"some_field": "value"}
        assert handler.can_handle(message) is False


class TestExtractPageToken:
    """Test the _extract_page_token method."""

    def test_extract_page_token_from_resource_uri(self, handler):
        """Test extracting page token from valid resource URI."""
        uri = "https://www.googleapis.com/drive/v3/changes?pageToken=abc123xyz"
        token = handler._extract_page_token(uri)
        assert token == "abc123xyz"

    def test_extract_page_token_with_multiple_params(self, handler):
        """Test extracting page token from URI with multiple parameters."""
        uri = "https://www.googleapis.com/drive/v3/changes?pageToken=token123&includeRemoved=true"
        token = handler._extract_page_token(uri)
        assert token == "token123"

    def test_extract_page_token_invalid_uri(self, handler):
        """Test handling malformed URIs gracefully."""
        invalid_uris = [
            "",
            None,
            "not_a_uri",
            "https://www.googleapis.com/drive/v3/changes",  # No query params
            "https://www.googleapis.com/drive/v3/changes?other=param",  # No pageToken
        ]
        for uri in invalid_uris:
            token = handler._extract_page_token(uri)
            assert token is None

    def test_extract_page_token_with_special_characters(self, handler):
        """Test extracting token with URL-encoded special characters."""
        uri = "https://www.googleapis.com/drive/v3/changes?pageToken=abc%2F123%3Dxyz"
        token = handler._extract_page_token(uri)
        assert token == "abc/123=xyz"  # Should be URL-decoded


class TestProcessMessage:
    """Test the process_message method."""

    @pytest.mark.asyncio
    async def test_process_webhook_notification(
        self, handler, valid_webhook_message, connection_config, mock_api_client, sample_file_data
    ):
        """Test processing a valid webhook notification."""
        # Setup mock API response
        mock_api_client.list_changes.return_value = {
            "changes": [
                {
                    "fileId": "file_123",
                    "file": sample_file_data,
                    "changeType": "file",
                    "removed": False,
                }
            ],
            "newStartPageToken": "new_token_xyz",
        }

        # Patch the API client creation
        with patch(
            "google_drive_worker.handlers.webhook.GoogleDriveAPIClient",
            return_value=mock_api_client,
        ):
            records = []
            async for record in handler.process_message(
                valid_webhook_message, connection_config
            ):
                records.append(record)

        # Verify results
        assert len(records) == 1
        assert records[0]["resource_id"] == "file_123"
        assert records[0]["resource_type"] == "file"
        assert records[0]["integration_connection_id"] == "conn_abc123"
        assert records[0]["customer_id"] == "cust_123"
        assert records[0]["idempotency_key"] == "google-drive:conn_abc123:file:file_123"

        # Verify API client was closed
        mock_api_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_webhook_skips_removed_files(
        self, handler, valid_webhook_message, connection_config, mock_api_client
    ):
        """Test that removed files are skipped during processing."""
        # Setup mock API response with removed file
        mock_api_client.list_changes.return_value = {
            "changes": [
                {
                    "fileId": "file_123",
                    "removed": True,  # File was removed
                },
                {
                    "fileId": "file_456",
                    "file": {
                        "id": "file_456",
                        "name": "Active.docx",
                        "mimeType": "application/vnd.google-apps.document",
                    },
                    "removed": False,
                },
            ],
        }

        with patch(
            "google_drive_worker.handlers.webhook.GoogleDriveAPIClient",
            return_value=mock_api_client,
        ):
            records = []
            async for record in handler.process_message(
                valid_webhook_message, connection_config
            ):
                records.append(record)

        # Should only yield the non-removed file
        assert len(records) == 1
        assert records[0]["resource_id"] == "file_456"

    @pytest.mark.asyncio
    async def test_process_webhook_handles_empty_changes(
        self, handler, valid_webhook_message, connection_config, mock_api_client
    ):
        """Test handling webhook with no changes."""
        # Setup mock API response with no changes
        mock_api_client.list_changes.return_value = {
            "changes": [],
            "newStartPageToken": "new_token_xyz",
        }

        with patch(
            "google_drive_worker.handlers.webhook.GoogleDriveAPIClient",
            return_value=mock_api_client,
        ):
            records = []
            async for record in handler.process_message(
                valid_webhook_message, connection_config
            ):
                records.append(record)

        # Should yield no records
        assert len(records) == 0
        mock_api_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_webhook_api_client_cleanup(
        self, handler, valid_webhook_message, connection_config, mock_api_client
    ):
        """Test that API client is properly cleaned up even on error."""
        # Setup mock to raise an error
        mock_api_client.list_changes.side_effect = Exception("API error")

        with patch(
            "google_drive_worker.handlers.webhook.GoogleDriveAPIClient",
            return_value=mock_api_client,
        ):
            with pytest.raises(RetriableError, match="Failed to fetch changes"):
                records = []
                async for record in handler.process_message(
                    valid_webhook_message, connection_config
                ):
                    records.append(record)

        # Verify API client was still closed
        mock_api_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_channel_stop_notification(
        self, handler, connection_config, mock_api_client
    ):
        """Test handling channel stop notification."""
        stop_message = {
            "message_id": str(uuid.uuid4()),
            "customer_id": "cust_123",
            "integration_connection_id": "conn_abc123",
            "payload": {
                "kind": "drive#stop",  # Stop notification
                "id": "channel_id_123",
            },
        }

        with patch(
            "google_drive_worker.handlers.webhook.GoogleDriveAPIClient",
            return_value=mock_api_client,
        ):
            records = []
            async for record in handler.process_message(stop_message, connection_config):
                records.append(record)

        # Should yield no records for stop notification
        assert len(records) == 0
        # API client should not have been created/called
        mock_api_client.list_changes.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_webhook_no_page_token(
        self, handler, connection_config, mock_api_client
    ):
        """Test handling webhook without page token."""
        message = {
            "message_id": str(uuid.uuid4()),
            "customer_id": "cust_123",
            "integration_connection_id": "conn_abc123",
            "payload": {
                "kind": "drive#change",
                "id": "channel_id_123",
                "resourceUri": "https://www.googleapis.com/drive/v3/changes",  # No pageToken
            },
        }

        with patch(
            "google_drive_worker.handlers.webhook.GoogleDriveAPIClient",
            return_value=mock_api_client,
        ):
            records = []
            async for record in handler.process_message(message, connection_config):
                records.append(record)

        # Should yield no records and not call API
        assert len(records) == 0
        mock_api_client.list_changes.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_webhook_filters_by_settings(
        self, handler, valid_webhook_message, mock_api_client, sample_file_data, sample_folder_data
    ):
        """Test that files/folders are filtered based on connection settings."""
        # Configuration that excludes folders
        config_no_folders = {
            "access_token": "test_token",
            "refresh_token": "test_refresh",
            "client_id": "test_client",
            "client_secret": "test_secret",
            "settings": {
                "sync_files": True,
                "sync_folders": False,  # Don't sync folders
                "sync_shared_drives": True,
            },
        }

        # Setup mock API response with both file and folder
        mock_api_client.list_changes.return_value = {
            "changes": [
                {
                    "fileId": "file_123",
                    "file": sample_file_data,
                    "removed": False,
                },
                {
                    "fileId": "folder_456",
                    "file": sample_folder_data,
                    "removed": False,
                },
            ],
        }

        with patch(
            "google_drive_worker.handlers.webhook.GoogleDriveAPIClient",
            return_value=mock_api_client,
        ):
            records = []
            async for record in handler.process_message(
                valid_webhook_message, config_no_folders
            ):
                records.append(record)

        # Should only yield the file, not the folder
        assert len(records) == 1
        assert records[0]["resource_id"] == "file_123"
        assert records[0]["resource_type"] == "file"

    @pytest.mark.asyncio
    async def test_process_webhook_with_multiple_changes(
        self, handler, valid_webhook_message, connection_config, mock_api_client
    ):
        """Test processing webhook with multiple file changes."""
        # Create multiple file changes
        changes = []
        for i in range(5):
            changes.append({
                "fileId": f"file_{i}",
                "file": {
                    "id": f"file_{i}",
                    "name": f"Document_{i}.docx",
                    "mimeType": "application/vnd.google-apps.document",
                    "createdTime": "2025-01-15T10:00:00Z",
                    "modifiedTime": "2025-01-20T15:30:00Z",
                },
                "changeType": "file",
                "removed": False,
            })

        mock_api_client.list_changes.return_value = {
            "changes": changes,
            "newStartPageToken": "new_token_xyz",
        }

        with patch(
            "google_drive_worker.handlers.webhook.GoogleDriveAPIClient",
            return_value=mock_api_client,
        ):
            records = []
            async for record in handler.process_message(
                valid_webhook_message, connection_config
            ):
                records.append(record)

        # Should yield all 5 records
        assert len(records) == 5
        for i, record in enumerate(records):
            assert record["resource_id"] == f"file_{i}"


class TestGenerateIdempotencyKey:
    """Test the generate_idempotency_key method."""

    def test_generate_idempotency_key(self, handler):
        """Test generating idempotency key with correct pattern."""
        key = handler.generate_idempotency_key("conn_123", "file", "file_abc")
        assert key == "google-drive:conn_123:file:file_abc"

    def test_generate_idempotency_key_folder(self, handler):
        """Test generating idempotency key for folder."""
        key = handler.generate_idempotency_key("conn_456", "folder", "folder_xyz")
        assert key == "google-drive:conn_456:folder:folder_xyz"

    def test_generate_idempotency_key_deterministic(self, handler):
        """Test that same inputs always produce same key."""
        key1 = handler.generate_idempotency_key("conn_123", "file", "file_abc")
        key2 = handler.generate_idempotency_key("conn_123", "file", "file_abc")
        assert key1 == key2


class TestValidateWebhookMessage:
    """Test the _validate_webhook_message method."""

    def test_validate_valid_message(self, handler, valid_webhook_message):
        """Test validation passes for valid message."""
        # Should not raise
        handler._validate_webhook_message(valid_webhook_message)

    def test_validate_missing_required_fields(self, handler):
        """Test validation fails for missing required fields."""
        invalid_message = {
            "message_id": "msg_123",
            # Missing customer_id, integration_connection_id, payload
        }
        with pytest.raises(ValidationError, match="Missing required fields"):
            handler._validate_webhook_message(invalid_message)

    def test_validate_invalid_payload_type(self, handler):
        """Test validation fails for non-dict payload."""
        invalid_message = {
            "message_id": "msg_123",
            "customer_id": "cust_123",
            "integration_connection_id": "conn_123",
            "payload": "not_a_dict",  # Should be dict
        }
        with pytest.raises(ValidationError, match="payload must be a dictionary"):
            handler._validate_webhook_message(invalid_message)

    def test_validate_missing_channel_id(self, handler):
        """Test validation fails for missing channel ID in payload."""
        invalid_message = {
            "message_id": "msg_123",
            "customer_id": "cust_123",
            "integration_connection_id": "conn_123",
            "payload": {
                # Missing "id" field
                "resourceUri": "https://example.com",
            },
        }
        with pytest.raises(ValidationError, match="Missing channel ID"):
            handler._validate_webhook_message(invalid_message)

    def test_validate_missing_resource_uri_for_change(self, handler):
        """Test validation fails for missing resourceUri in change notification."""
        invalid_message = {
            "message_id": "msg_123",
            "customer_id": "cust_123",
            "integration_connection_id": "conn_123",
            "payload": {
                "kind": "drive#change",  # Not a stop notification
                "id": "channel_123",
                # Missing resourceUri
            },
        }
        with pytest.raises(ValidationError, match="Missing resourceUri"):
            handler._validate_webhook_message(invalid_message)

    def test_validate_stop_notification_without_resource_uri(self, handler):
        """Test validation passes for stop notification without resourceUri."""
        stop_message = {
            "message_id": "msg_123",
            "customer_id": "cust_123",
            "integration_connection_id": "conn_123",
            "payload": {
                "kind": "drive#stop",
                "id": "channel_123",
                # resourceUri not required for stop notifications
            },
        }
        # Should not raise
        handler._validate_webhook_message(stop_message)


class TestWebhookMetadata:
    """Test that webhook records have correct metadata."""

    @pytest.mark.asyncio
    async def test_webhook_metadata_in_record(
        self, handler, valid_webhook_message, connection_config, mock_api_client, sample_file_data
    ):
        """Test that webhook records have correct metadata fields."""
        mock_api_client.list_changes.return_value = {
            "changes": [
                {
                    "fileId": "file_123",
                    "file": sample_file_data,
                    "changeType": "file",
                    "removed": False,
                }
            ],
        }

        with patch(
            "google_drive_worker.handlers.webhook.GoogleDriveAPIClient",
            return_value=mock_api_client,
        ):
            records = []
            async for record in handler.process_message(
                valid_webhook_message, connection_config
            ):
                records.append(record)

        assert len(records) == 1
        metadata = records[0]["metadata"]
        assert metadata["source"] == "webhook"
        assert metadata["trigger_type"] == "push_notification"
        assert metadata["change_type"] == "file"
        assert metadata["source_format"] == "google_drive_api_v3"
        assert metadata["transformation_version"] == "1.0.0"
        assert "page_token" in metadata