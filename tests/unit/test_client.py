"""Unit tests for Google Drive API client."""

import pytest
import httpx
import respx
from unittest.mock import AsyncMock, patch, MagicMock

from google_drive_worker.client.drive_api import GoogleDriveAPIClient
from google_drive_worker.client.auth import GoogleOAuthClient
from google_drive_worker.utils.errors import (
    RateLimitError,
    RetriableError,
    TerminalError,
    AuthenticationError,
)


@pytest.mark.asyncio
class TestGoogleDriveAPIClient:
    """Test cases for GoogleDriveAPIClient."""

    @pytest.fixture
    def credentials(self):
        """Sample OAuth credentials."""
        return {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
        }

    @pytest.fixture
    def api_client(self, credentials):
        """Create a Google Drive API client instance."""
        return GoogleDriveAPIClient(
            access_token=credentials["access_token"],
            refresh_token=credentials["refresh_token"],
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
        )

    @pytest.fixture
    def mock_oauth_client(self):
        """Create a mock OAuth client."""
        mock = AsyncMock(spec=GoogleOAuthClient)
        mock.refresh_access_token = AsyncMock(return_value="new_access_token")
        return mock

    # list_files tests

    @respx.mock
    async def test_list_files_success(self, api_client):
        """Test successful file listing."""
        # Mock response
        files_response = {
            "files": [
                {
                    "id": "file1",
                    "name": "Document.pdf",
                    "mimeType": "application/pdf",
                    "size": "1024",
                    "modifiedTime": "2025-01-01T00:00:00.000Z",
                },
                {
                    "id": "file2",
                    "name": "Spreadsheet.xlsx",
                    "mimeType": "application/vnd.ms-excel",
                    "size": "2048",
                    "modifiedTime": "2025-01-02T00:00:00.000Z",
                },
            ],
            "nextPageToken": "next_page_token_123",
        }

        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(200, json=files_response)
        )

        # Call method
        result = await api_client.list_files(page_size=100)

        # Verify result
        assert len(result["files"]) == 2
        assert result["files"][0]["id"] == "file1"
        assert result["files"][1]["name"] == "Spreadsheet.xlsx"
        assert result["nextPageToken"] == "next_page_token_123"

        # Verify request
        request = respx.calls.last.request
        assert request.headers["Authorization"] == "Bearer test_access_token"
        assert "pageSize=100" in str(request.url)
        assert "supportsAllDrives=true" in str(request.url)

    @respx.mock
    async def test_list_files_with_pagination(self, api_client):
        """Test file listing with pagination token."""
        # Mock response for second page
        files_response = {
            "files": [
                {"id": "file3", "name": "Page2Doc.txt"},
            ],
            # No nextPageToken means last page
        }

        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(200, json=files_response)
        )

        # Call with page token
        result = await api_client.list_files(
            page_size=50,
            page_token="previous_page_token",
        )

        # Verify
        assert len(result["files"]) == 1
        assert result["files"][0]["id"] == "file3"
        assert "nextPageToken" not in result

        # Verify page token in request
        request = respx.calls.last.request
        assert "pageToken=previous_page_token" in str(request.url)

    @respx.mock
    async def test_list_files_with_query(self, api_client):
        """Test file listing with search query."""
        # Mock response
        files_response = {"files": [], "nextPageToken": None}

        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(200, json=files_response)
        )

        # Call with query
        await api_client.list_files(
            query="name contains 'report' and mimeType = 'application/pdf'",
            include_trashed=False,
        )

        # Verify query was modified to exclude trashed
        request = respx.calls.last.request
        query_param = request.url.params.get("q")
        assert "name contains 'report'" in query_param
        assert "mimeType = 'application/pdf'" in query_param
        assert "trashed = false" in query_param

    @respx.mock
    async def test_list_files_rate_limit(self, api_client):
        """Test handling of rate limit (429) response."""
        # Mock 429 response
        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(
                429,
                headers={"Retry-After": "45"},
                text="User rate limit exceeded",
            )
        )

        # Call should raise RateLimitError
        with pytest.raises(RateLimitError) as exc_info:
            await api_client.list_files()

        # Verify error details
        assert exc_info.value.retry_after == 45
        assert "Rate limit exceeded" in str(exc_info.value)

    # get_file tests

    @respx.mock
    async def test_get_file_success(self, api_client):
        """Test successful file metadata retrieval."""
        # Mock response
        file_response = {
            "id": "file123",
            "name": "Important.docx",
            "mimeType": "application/vnd.google-apps.document",
            "size": "5000",
            "modifiedTime": "2025-01-15T10:30:00.000Z",
            "owners": [{"emailAddress": "owner@example.com"}],
            "webViewLink": "https://docs.google.com/document/d/file123",
        }

        respx.get("https://www.googleapis.com/drive/v3/files/file123").mock(
            return_value=httpx.Response(200, json=file_response)
        )

        # Call method
        result = await api_client.get_file("file123")

        # Verify result
        assert result["id"] == "file123"
        assert result["name"] == "Important.docx"
        assert result["mimeType"] == "application/vnd.google-apps.document"

    @respx.mock
    async def test_get_file_not_found(self, api_client):
        """Test handling of file not found (404)."""
        # Mock 404 response
        error_response = {
            "error": {
                "code": 404,
                "message": "File not found: file456",
            }
        }

        respx.get("https://www.googleapis.com/drive/v3/files/file456").mock(
            return_value=httpx.Response(404, json=error_response)
        )

        # Call should raise TerminalError
        with pytest.raises(TerminalError) as exc_info:
            await api_client.get_file("file456")

        # Verify error
        assert "File not found" in str(exc_info.value)
        assert not exc_info.value.retriable

    # download_file tests

    @respx.mock
    async def test_download_file_success(self, api_client):
        """Test successful file download."""
        # Mock binary response (PDF content)
        pdf_content = b"%PDF-1.4\nPDF binary content here..."

        respx.get("https://www.googleapis.com/drive/v3/files/file123").mock(
            return_value=httpx.Response(200, content=pdf_content)
        )

        # Call method
        result = await api_client.download_file("file123")

        # Verify result
        assert result == pdf_content
        assert isinstance(result, bytes)

        # Verify request used alt=media
        request = respx.calls.last.request
        assert "alt=media" in str(request.url)

    @respx.mock
    async def test_download_file_image(self, api_client):
        """Test downloading image file."""
        # Mock image binary content
        image_content = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR..."

        respx.get("https://www.googleapis.com/drive/v3/files/img456").mock(
            return_value=httpx.Response(200, content=image_content)
        )

        # Call method
        result = await api_client.download_file("img456")

        # Verify result
        assert result == image_content
        assert isinstance(result, bytes)

    @respx.mock
    async def test_download_file_not_found(self, api_client):
        """Test handling of file not found (404) during download."""
        # Mock 404 response
        error_response = {
            "error": {
                "code": 404,
                "message": "File not found: file789",
            }
        }

        respx.get("https://www.googleapis.com/drive/v3/files/file789").mock(
            return_value=httpx.Response(404, json=error_response)
        )

        # Call should raise TerminalError
        with pytest.raises(TerminalError) as exc_info:
            await api_client.download_file("file789")

        assert "File not found" in str(exc_info.value)

    @respx.mock
    async def test_download_file_rate_limit(self, api_client):
        """Test handling of rate limit during download."""
        respx.get("https://www.googleapis.com/drive/v3/files/file_limited").mock(
            return_value=httpx.Response(
                429,
                headers={"Retry-After": "30"},
                text="Rate limit exceeded",
            )
        )

        with pytest.raises(RateLimitError) as exc_info:
            await api_client.download_file("file_limited")

        assert exc_info.value.retry_after == 30

    # export_file tests

    @respx.mock
    async def test_export_file_success(self, api_client):
        """Test successful file export."""
        # Mock binary response
        pdf_content = b"%PDF-1.4\n...binary content..."

        respx.get("https://www.googleapis.com/drive/v3/files/doc123/export").mock(
            return_value=httpx.Response(200, content=pdf_content)
        )

        # Call method
        result = await api_client.export_file("doc123", "application/pdf")

        # Verify result
        assert result == pdf_content
        assert isinstance(result, bytes)

        # Verify request
        request = respx.calls.last.request
        assert "mimeType=application%2Fpdf" in str(request.url)

    @respx.mock
    async def test_export_file_unsupported_format(self, api_client):
        """Test export with unsupported format."""
        # Mock 400 bad request
        error_response = {
            "error": {
                "code": 400,
                "message": "Export only supports Google Workspace documents",
            }
        }

        respx.get("https://www.googleapis.com/drive/v3/files/image123/export").mock(
            return_value=httpx.Response(400, json=error_response)
        )

        # Call should raise TerminalError
        with pytest.raises(TerminalError) as exc_info:
            await api_client.export_file("image123", "application/pdf")

        # Verify error
        assert "Export only supports" in str(exc_info.value)

    # Authentication tests

    @respx.mock
    async def test_auto_token_refresh_on_401(self, credentials):
        """Test automatic token refresh on 401 error."""
        # Create client with mock OAuth client
        mock_oauth = AsyncMock(spec=GoogleOAuthClient)
        mock_oauth.refresh_access_token = AsyncMock(return_value="refreshed_token")

        api_client = GoogleDriveAPIClient(
            access_token=credentials["access_token"],
            refresh_token=credentials["refresh_token"],
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
            oauth_client=mock_oauth,
        )

        # First request returns 401, second succeeds
        files_response = {"files": [{"id": "file1"}]}

        route = respx.get("https://www.googleapis.com/drive/v3/files")
        route.side_effect = [
            httpx.Response(401, json={"error": "Invalid token"}),
            httpx.Response(200, json=files_response),
        ]

        # Call method
        result = await api_client.list_files()

        # Verify token was refreshed
        mock_oauth.refresh_access_token.assert_called_once_with(
            credentials["refresh_token"],
            credentials["client_id"],
            credentials["client_secret"],
        )

        # Verify request succeeded after refresh
        assert result["files"][0]["id"] == "file1"

        # Verify both requests were made
        assert len(respx.calls) == 2
        # Second request should use new token
        assert respx.calls[1].request.headers["Authorization"] == "Bearer refreshed_token"

    @respx.mock
    async def test_token_refresh_failure(self, credentials):
        """Test handling when token refresh fails."""
        # Create client with mock OAuth client that fails
        mock_oauth = AsyncMock(spec=GoogleOAuthClient)
        mock_oauth.refresh_access_token = AsyncMock(
            side_effect=AuthenticationError("Refresh token expired")
        )

        api_client = GoogleDriveAPIClient(
            access_token=credentials["access_token"],
            refresh_token=credentials["refresh_token"],
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
            oauth_client=mock_oauth,
        )

        # Mock 401 response
        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(401)
        )

        # Call should raise AuthenticationError from refresh failure
        with pytest.raises(AuthenticationError) as exc_info:
            await api_client.list_files()

        # Verify it's the refresh error
        assert "Refresh token expired" in str(exc_info.value)

    @respx.mock
    async def test_no_retry_after_token_already_refreshed(self, credentials):
        """Test that token is only refreshed once per session."""
        # Create client with mock OAuth client
        mock_oauth = AsyncMock(spec=GoogleOAuthClient)
        mock_oauth.refresh_access_token = AsyncMock(return_value="refreshed_token")

        api_client = GoogleDriveAPIClient(
            access_token=credentials["access_token"],
            refresh_token=credentials["refresh_token"],
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
            oauth_client=mock_oauth,
        )

        # Mark as already refreshed
        api_client._token_refreshed = True

        # Mock 401 response
        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(401)
        )

        # Call should raise AuthenticationError without refresh attempt
        with pytest.raises(AuthenticationError) as exc_info:
            await api_client.list_files()

        # Verify token refresh was NOT called
        mock_oauth.refresh_access_token.assert_not_called()
        assert "Authentication failed with status 401" in str(exc_info.value)

    # Changes API tests

    @respx.mock
    async def test_list_changes_success(self, api_client):
        """Test successful change listing."""
        # Mock response
        changes_response = {
            "changes": [
                {
                    "file": {"id": "file1", "name": "Modified.doc"},
                    "removed": False,
                    "changeType": "file",
                },
                {
                    "file": None,
                    "fileId": "file2",
                    "removed": True,
                    "changeType": "file",
                },
            ],
            "nextPageToken": "next_changes_token",
            "newStartPageToken": None,
        }

        respx.get("https://www.googleapis.com/drive/v3/changes").mock(
            return_value=httpx.Response(200, json=changes_response)
        )

        # Call method
        result = await api_client.list_changes("start_page_token_123")

        # Verify result
        assert len(result["changes"]) == 2
        assert result["changes"][0]["removed"] is False
        assert result["changes"][1]["removed"] is True
        assert result["nextPageToken"] == "next_changes_token"

    @respx.mock
    async def test_get_start_page_token_success(self, api_client):
        """Test getting start page token for changes."""
        # Mock response
        token_response = {
            "startPageToken": "start_token_abc123xyz",
        }

        respx.get("https://www.googleapis.com/drive/v3/changes/startPageToken").mock(
            return_value=httpx.Response(200, json=token_response)
        )

        # Call method
        result = await api_client.get_start_page_token()

        # Verify result
        assert result == "start_token_abc123xyz"

    @respx.mock
    async def test_get_start_page_token_missing(self, api_client):
        """Test handling when start page token is missing from response."""
        # Mock response without token
        respx.get("https://www.googleapis.com/drive/v3/changes/startPageToken").mock(
            return_value=httpx.Response(200, json={})
        )

        # Call should raise TerminalError
        with pytest.raises(TerminalError) as exc_info:
            await api_client.get_start_page_token()

        assert "Failed to get start page token" in str(exc_info.value)

    # Revisions tests

    @respx.mock
    async def test_list_revisions_success(self, api_client):
        """Test successful revision listing."""
        # Mock response
        revisions_response = {
            "revisions": [
                {
                    "id": "rev1",
                    "modifiedTime": "2025-01-01T10:00:00.000Z",
                    "lastModifyingUser": {"emailAddress": "user1@example.com"},
                },
                {
                    "id": "rev2",
                    "modifiedTime": "2025-01-02T10:00:00.000Z",
                    "lastModifyingUser": {"emailAddress": "user2@example.com"},
                },
            ]
        }

        respx.get("https://www.googleapis.com/drive/v3/files/file123/revisions").mock(
            return_value=httpx.Response(200, json=revisions_response)
        )

        # Call method
        result = await api_client.list_revisions("file123")

        # Verify result
        assert len(result) == 2
        assert result[0]["id"] == "rev1"
        assert result[1]["id"] == "rev2"

    # Error handling tests

    @respx.mock
    async def test_server_error_500(self, api_client):
        """Test handling of server error (500)."""
        # Mock 500 response
        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )

        # Call should raise RetriableError
        with pytest.raises(RetriableError) as exc_info:
            await api_client.list_files()

        # Verify error is retriable
        assert exc_info.value.retriable
        assert "Google API error: 500" in str(exc_info.value)

    @respx.mock
    async def test_bad_gateway_502(self, api_client):
        """Test handling of bad gateway (502)."""
        # Mock 502 response
        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(502, text="Bad Gateway")
        )

        # Call should raise RetriableError
        with pytest.raises(RetriableError) as exc_info:
            await api_client.list_files()

        assert exc_info.value.retriable

    @respx.mock
    async def test_network_error(self, api_client):
        """Test handling of network errors."""
        # Mock network error
        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            side_effect=httpx.NetworkError("Connection reset")
        )

        # Call should raise RetriableError
        with pytest.raises(RetriableError) as exc_info:
            await api_client.list_files()

        assert exc_info.value.retriable
        assert "Network error" in str(exc_info.value)

    @respx.mock
    async def test_timeout_error(self, api_client):
        """Test handling of timeout errors."""
        # Mock timeout
        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            side_effect=httpx.TimeoutException("Request timeout")
        )

        # Call should raise RetriableError
        with pytest.raises(RetriableError) as exc_info:
            await api_client.list_files()

        assert exc_info.value.retriable
        assert "timed out" in str(exc_info.value)

    # Context manager tests

    async def test_context_manager(self, credentials):
        """Test API client as async context manager."""
        # Create and use client in context
        async with GoogleDriveAPIClient(
            access_token=credentials["access_token"],
            refresh_token=credentials["refresh_token"],
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
        ) as client:
            assert client._http_client is not None

        # HTTP client should be closed after context exit
        # (We can't directly test if it's closed, but the context manager should handle it)

    @respx.mock
    async def test_custom_fields_parameter(self, api_client):
        """Test custom fields parameter in list_files."""
        # Mock response
        files_response = {"files": []}

        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(200, json=files_response)
        )

        # Call with custom fields
        await api_client.list_files(fields="files(id,name),nextPageToken")

        # Verify fields parameter
        request = respx.calls.last.request
        assert "fields=files%28id%2Cname%29%2CnextPageToken" in str(request.url)

    @respx.mock
    async def test_shared_drives_parameter(self, api_client):
        """Test shared drives inclusion parameter."""
        # Mock response
        files_response = {"files": []}

        respx.get("https://www.googleapis.com/drive/v3/files").mock(
            return_value=httpx.Response(200, json=files_response)
        )

        # Call without shared drives
        await api_client.list_files(include_shared_drives=False)

        # Verify parameters
        request = respx.calls.last.request
        assert "supportsAllDrives=false" in str(request.url)
        assert "includeItemsFromAllDrives=false" in str(request.url)