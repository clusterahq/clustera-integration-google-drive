"""Unit tests for Google Drive handlers."""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from google_drive_worker.handlers.trigger import GoogleDriveTriggerHandler
from google_drive_worker.handlers.base import BaseIntegrationHandler
from google_drive_worker.config import GoogleDriveAPIConfig
from google_drive_worker.utils.errors import ValidationError


@pytest.fixture
def api_config():
    """Create test API configuration."""
    return GoogleDriveAPIConfig()


@pytest.fixture
def trigger_handler(api_config):
    """Create trigger handler for testing."""
    return GoogleDriveTriggerHandler(api_config=api_config)


@pytest.fixture
def mock_api_client():
    """Create mock GoogleDriveAPIClient."""
    client = AsyncMock()
    client.close = AsyncMock()
    client.list_files = AsyncMock()
    client.list_changes = AsyncMock()
    client.get_start_page_token = AsyncMock()
    return client


@pytest.fixture
def valid_trigger_message():
    """Create a valid trigger message."""
    return {
        "message_id": str(uuid.uuid4()),
        "customer_id": "cust_123",
        "integration_id": "google-drive",
        "integration_connection_id": "conn_abc",
        "trigger_type": "full_sync",
        "last_cursor": {},
        "rate_limit_hints": {
            "remaining_quota": 280
        }
    }


@pytest.fixture
def connection_config():
    """Create test connection configuration."""
    return {
        "connection_id": "conn_abc",
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


class TestGoogleDriveTriggerHandler:
    """Test GoogleDriveTriggerHandler class."""

    @pytest.mark.asyncio
    async def test_can_handle_with_google_drive_id(self, trigger_handler, valid_trigger_message):
        """Test handler accepts Google Drive messages."""
        result = await trigger_handler.can_handle(valid_trigger_message)
        assert result is True

    @pytest.mark.asyncio
    async def test_rejects_other_integrations(self, trigger_handler):
        """Test handler rejects other integration messages."""
        # Test with Circle.so
        message = {"integration_id": "circle"}
        result = await trigger_handler.can_handle(message)
        assert result is False

        # Test with GitHub
        message = {"integration_id": "github"}
        result = await trigger_handler.can_handle(message)
        assert result is False

        # Test with Slack
        message = {"integration_id": "slack"}
        result = await trigger_handler.can_handle(message)
        assert result is False

    def test_generate_idempotency_key(self, trigger_handler):
        """Test idempotency key generation follows pattern."""
        key = trigger_handler.generate_idempotency_key(
            connection_id="conn_abc",
            resource_type="file",
            resource_id="file_123"
        )
        assert key == "google-drive:conn_abc:file:file_123"

        # Test with folder
        key = trigger_handler.generate_idempotency_key(
            connection_id="conn_xyz",
            resource_type="folder",
            resource_id="folder_456"
        )
        assert key == "google-drive:conn_xyz:folder:folder_456"

    def test_validate_trigger_message_success(self, trigger_handler, valid_trigger_message):
        """Test validation passes for valid message."""
        # Should not raise any exception
        trigger_handler._validate_trigger_message(valid_trigger_message)

    def test_validate_trigger_message_missing_fields(self, trigger_handler):
        """Test validation fails for missing required fields."""
        # Missing message_id
        message = {
            "customer_id": "cust_123",
            "integration_id": "google-drive",
            "integration_connection_id": "conn_abc",
            "trigger_type": "full_sync",
        }
        with pytest.raises(ValidationError) as exc:
            trigger_handler._validate_trigger_message(message)
        assert "Missing required fields: message_id" in str(exc.value)

        # Missing multiple fields
        message = {
            "integration_id": "google-drive",
            "trigger_type": "full_sync",
        }
        with pytest.raises(ValidationError) as exc:
            trigger_handler._validate_trigger_message(message)
        assert "Missing required fields" in str(exc.value)
        assert "message_id" in str(exc.value)
        assert "customer_id" in str(exc.value)

    def test_validate_trigger_message_invalid_trigger_type(self, trigger_handler, valid_trigger_message):
        """Test validation fails for invalid trigger type."""
        valid_trigger_message["trigger_type"] = "invalid_type"
        with pytest.raises(ValidationError) as exc:
            trigger_handler._validate_trigger_message(valid_trigger_message)
        assert "Invalid trigger_type: invalid_type" in str(exc.value)
        assert "Must be one of: full_sync, incremental, poll" in str(exc.value)

    @pytest.mark.asyncio
    async def test_process_full_sync(self, trigger_handler, valid_trigger_message,
                                     connection_config, mock_api_client):
        """Test full sync processing."""
        # Mock API response
        mock_api_client.list_files.side_effect = [
            {
                "files": [
                    {
                        "id": "file_1",
                        "name": "Document.docx",
                        "mimeType": "application/vnd.google-apps.document",
                        "createdTime": "2025-01-01T10:00:00Z",
                        "modifiedTime": "2025-01-15T15:30:00Z",
                        "size": "12345",
                        "webViewLink": "https://drive.google.com/file/d/file_1",
                        "owners": [{"emailAddress": "user@example.com", "displayName": "Test User"}],
                    },
                    {
                        "id": "folder_1",
                        "name": "My Folder",
                        "mimeType": "application/vnd.google-apps.folder",
                        "createdTime": "2025-01-01T09:00:00Z",
                        "modifiedTime": "2025-01-10T12:00:00Z",
                        "webViewLink": "https://drive.google.com/drive/folders/folder_1",
                    },
                ],
                "nextPageToken": None
            }
        ]

        with patch('google_drive_worker.handlers.trigger.GoogleDriveAPIClient') as MockClient:
            MockClient.return_value = mock_api_client

            # Process message
            records = []
            async for record in trigger_handler.process_message(valid_trigger_message, connection_config):
                records.append(record)

            # Verify results
            assert len(records) == 2

            # Check first file
            file_record = records[0]
            assert file_record["resource_type"] == "file"
            assert file_record["resource_id"] == "file_1"
            assert file_record["integration_connection_id"] == "conn_abc"
            assert file_record["idempotency_key"] == "google-drive:conn_abc:file:file_1"
            assert file_record["data"]["name"] == "Document.docx"
            assert file_record["data"]["type"] == "file"
            assert file_record["metadata"]["trigger_type"] == "full_sync"

            # Check folder
            folder_record = records[1]
            assert folder_record["resource_type"] == "folder"
            assert folder_record["resource_id"] == "folder_1"
            assert folder_record["idempotency_key"] == "google-drive:conn_abc:folder:folder_1"
            assert folder_record["data"]["name"] == "My Folder"
            assert folder_record["data"]["type"] == "folder"

            # Verify API client was closed
            mock_api_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_incremental_sync(self, trigger_handler, connection_config, mock_api_client):
        """Test incremental sync processing."""
        message = {
            "message_id": str(uuid.uuid4()),
            "customer_id": "cust_123",
            "integration_id": "google-drive",
            "integration_connection_id": "conn_abc",
            "trigger_type": "incremental",
            "last_cursor": {
                "page_token": "previous_token_123"
            }
        }

        # Mock API response
        mock_api_client.list_changes.return_value = {
            "changes": [
                {
                    "file": {
                        "id": "file_2",
                        "name": "Updated.xlsx",
                        "mimeType": "application/vnd.google-apps.spreadsheet",
                        "modifiedTime": "2025-01-20T10:00:00Z",
                    },
                    "removed": False,
                    "changeType": "file"
                },
                {
                    "file": {
                        "id": "file_3",
                        "name": "Deleted.pdf",
                    },
                    "removed": True,  # This should be skipped
                    "changeType": "file"
                }
            ],
            "nextPageToken": None,
            "newStartPageToken": "new_token_456"
        }

        with patch('google_drive_worker.handlers.trigger.GoogleDriveAPIClient') as MockClient:
            MockClient.return_value = mock_api_client

            # Process message
            records = []
            async for record in trigger_handler.process_message(message, connection_config):
                records.append(record)

            # Verify results
            assert len(records) == 1  # Only one file (removed file is skipped)

            record = records[0]
            assert record["resource_type"] == "file"
            assert record["resource_id"] == "file_2"
            assert record["data"]["name"] == "Updated.xlsx"
            assert record["metadata"]["trigger_type"] == "incremental"
            assert record["metadata"]["change_type"] == "file"

            # Verify API calls
            mock_api_client.list_changes.assert_called_once_with(
                page_token="previous_token_123",
                page_size=100,
                include_shared_drives=True
            )
            mock_api_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_incremental_no_page_token(self, trigger_handler, connection_config, mock_api_client):
        """Test incremental sync when no page token is provided."""
        message = {
            "message_id": str(uuid.uuid4()),
            "customer_id": "cust_123",
            "integration_id": "google-drive",
            "integration_connection_id": "conn_abc",
            "trigger_type": "incremental",
            "last_cursor": {}  # No page_token
        }

        # Mock API responses
        mock_api_client.get_start_page_token.return_value = "start_token_789"
        mock_api_client.list_changes.return_value = {
            "changes": [],
            "nextPageToken": None,
            "newStartPageToken": "new_token_999"
        }

        with patch('google_drive_worker.handlers.trigger.GoogleDriveAPIClient') as MockClient:
            MockClient.return_value = mock_api_client

            # Process message
            records = []
            async for record in trigger_handler.process_message(message, connection_config):
                records.append(record)

            # Verify API calls
            mock_api_client.get_start_page_token.assert_called_once_with(
                supports_all_drives=True
            )
            mock_api_client.list_changes.assert_called_once_with(
                page_token="start_token_789",
                page_size=100,
                include_shared_drives=True
            )
            mock_api_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_full_sync_pagination(self, trigger_handler, valid_trigger_message,
                                               connection_config, mock_api_client):
        """Test full sync handles pagination correctly."""
        # Mock paginated responses
        mock_api_client.list_files.side_effect = [
            {
                "files": [
                    {"id": f"file_{i}", "name": f"File {i}.txt", "mimeType": "text/plain"}
                    for i in range(1, 101)  # 100 files
                ],
                "nextPageToken": "page2"
            },
            {
                "files": [
                    {"id": f"file_{i}", "name": f"File {i}.txt", "mimeType": "text/plain"}
                    for i in range(101, 151)  # 50 more files
                ],
                "nextPageToken": None
            }
        ]

        with patch('google_drive_worker.handlers.trigger.GoogleDriveAPIClient') as MockClient:
            MockClient.return_value = mock_api_client

            # Process message
            records = []
            async for record in trigger_handler.process_message(valid_trigger_message, connection_config):
                records.append(record)

            # Verify results
            assert len(records) == 150  # Total files across both pages

            # Verify pagination
            assert mock_api_client.list_files.call_count == 2

            # Check first call
            first_call = mock_api_client.list_files.call_args_list[0]
            assert first_call[1]["page_token"] is None

            # Check second call
            second_call = mock_api_client.list_files.call_args_list[1]
            assert second_call[1]["page_token"] == "page2"

    @pytest.mark.asyncio
    async def test_process_sync_respects_settings(self, trigger_handler, valid_trigger_message,
                                                  mock_api_client):
        """Test sync respects connection settings for file/folder filtering."""
        # Configure to sync only folders
        connection_config = {
            "access_token": "token",
            "refresh_token": "refresh",
            "client_id": "client",
            "client_secret": "secret",
            "settings": {
                "sync_files": False,
                "sync_folders": True,
                "sync_shared_drives": False,
            }
        }

        mock_api_client.list_files.return_value = {
            "files": [
                {"id": "file_1", "name": "Doc.txt", "mimeType": "text/plain"},
                {"id": "folder_1", "name": "Folder", "mimeType": "application/vnd.google-apps.folder"},
            ],
            "nextPageToken": None
        }

        with patch('google_drive_worker.handlers.trigger.GoogleDriveAPIClient') as MockClient:
            MockClient.return_value = mock_api_client

            records = []
            async for record in trigger_handler.process_message(valid_trigger_message, connection_config):
                records.append(record)

            # Should only return the folder
            assert len(records) == 1
            assert records[0]["resource_type"] == "folder"
            assert records[0]["resource_id"] == "folder_1"

            # Verify query includes folder filter
            call_args = mock_api_client.list_files.call_args[1]
            assert "mimeType = 'application/vnd.google-apps.folder'" in call_args["query"]
            assert call_args["include_shared_drives"] is False

    def test_normalize_file(self, trigger_handler):
        """Test file normalization."""
        raw_file = {
            "id": "file_123",
            "name": "Document.docx",
            "mimeType": "application/vnd.google-apps.document",
            "createdTime": "2025-01-01T10:00:00Z",
            "modifiedTime": "2025-01-15T15:30:00Z",
            "size": "12345",
            "webViewLink": "https://drive.google.com/file/d/file_123",
            "parents": ["parent_folder_1"],
            "trashed": False,
            "owners": [
                {
                    "emailAddress": "owner@example.com",
                    "displayName": "File Owner"
                }
            ],
            "permissions": [
                {"role": "reader", "type": "user"},
                {"role": "writer", "type": "user"},
            ]
        }

        normalized = trigger_handler._normalize_file(raw_file)

        assert normalized["id"] == "file_123"
        assert normalized["name"] == "Document.docx"
        assert normalized["type"] == "file"
        assert normalized["mime_type"] == "application/vnd.google-apps.document"
        assert normalized["size_bytes"] == 12345
        assert normalized["parents"] == ["parent_folder_1"]
        assert normalized["owner"]["email"] == "owner@example.com"
        assert normalized["owner"]["name"] == "File Owner"
        assert normalized["permissions_count"] == 2
        assert normalized["trashed"] is False

    def test_normalize_folder(self, trigger_handler):
        """Test folder normalization."""
        raw_folder = {
            "id": "folder_456",
            "name": "My Folder",
            "mimeType": "application/vnd.google-apps.folder",
            "createdTime": "2025-01-01T09:00:00Z",
            "modifiedTime": "2025-01-10T12:00:00Z",
            "webViewLink": "https://drive.google.com/drive/folders/folder_456",
            "parents": [],
            "trashed": False,
        }

        normalized = trigger_handler._normalize_file(raw_folder)

        assert normalized["id"] == "folder_456"
        assert normalized["name"] == "My Folder"
        assert normalized["type"] == "folder"
        assert normalized["mime_type"] == "application/vnd.google-apps.folder"
        assert normalized["size_bytes"] is None
        assert normalized["parents"] == []

    @pytest.mark.asyncio
    async def test_error_handling_in_full_sync(self, trigger_handler, valid_trigger_message,
                                              connection_config, mock_api_client):
        """Test error handling during full sync."""
        # Mock API to raise an error
        from google_drive_worker.utils.errors import RetriableError
        mock_api_client.list_files.side_effect = RetriableError("API temporarily unavailable")

        with patch('google_drive_worker.handlers.trigger.GoogleDriveAPIClient') as MockClient:
            MockClient.return_value = mock_api_client

            with pytest.raises(RetriableError) as exc:
                async for record in trigger_handler.process_message(valid_trigger_message, connection_config):
                    pass

            assert "API temporarily unavailable" in str(exc.value)
            mock_api_client.close.assert_called_once()  # Ensure cleanup happens

    @pytest.mark.asyncio
    async def test_poll_trigger_type(self, trigger_handler, connection_config, mock_api_client):
        """Test that 'poll' trigger type is handled as incremental."""
        message = {
            "message_id": str(uuid.uuid4()),
            "customer_id": "cust_123",
            "integration_id": "google-drive",
            "integration_connection_id": "conn_abc",
            "trigger_type": "poll",  # Using 'poll' instead of 'incremental'
            "last_cursor": {}
        }

        mock_api_client.get_start_page_token.return_value = "token_123"
        mock_api_client.list_changes.return_value = {
            "changes": [],
            "nextPageToken": None,
            "newStartPageToken": "new_token"
        }

        with patch('google_drive_worker.handlers.trigger.GoogleDriveAPIClient') as MockClient:
            MockClient.return_value = mock_api_client

            records = []
            async for record in trigger_handler.process_message(message, connection_config):
                records.append(record)

            # Verify it was processed as incremental
            mock_api_client.get_start_page_token.assert_called_once()
            mock_api_client.list_changes.assert_called_once()


class TestBaseIntegrationHandler:
    """Test BaseIntegrationHandler methods."""

    def test_create_ingestion_envelope(self, trigger_handler):
        """Test envelope creation with proper metadata."""
        envelope = trigger_handler.create_ingestion_envelope(
            message_id="msg_123",
            customer_id="cust_456",
            connection_id="conn_789",
            resource_type="file",
            resource_id="file_abc",
            data={"name": "test.txt"},
            metadata={"custom": "value"}
        )

        assert envelope["message_id"] == "msg_123"
        assert envelope["customer_id"] == "cust_456"
        assert envelope["integration_connection_id"] == "conn_789"
        assert envelope["resource_type"] == "file"
        assert envelope["resource_id"] == "file_abc"
        assert envelope["idempotency_key"] == "google-drive:conn_789:file:file_abc"
        assert envelope["data"]["name"] == "test.txt"
        assert envelope["metadata"]["custom"] == "value"
        assert envelope["metadata"]["source_format"] == "google-drive_api"
        assert envelope["metadata"]["transformation_version"] == "1.0.0"
        assert "created_at" in envelope