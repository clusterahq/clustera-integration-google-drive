"""Unit tests for Google OAuth client."""

import pytest
import httpx
import respx
from unittest.mock import AsyncMock, patch

from google_drive_worker.client.auth import GoogleOAuthClient
from google_drive_worker.utils.errors import AuthenticationError, RetriableError


@pytest.mark.asyncio
class TestGoogleOAuthClient:
    """Test cases for GoogleOAuthClient."""

    @pytest.fixture
    def oauth_client(self):
        """Create an OAuth client instance."""
        return GoogleOAuthClient()

    @pytest.fixture
    def credentials(self):
        """Sample OAuth credentials."""
        return {
            "refresh_token": "test_refresh_token",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
        }

    @respx.mock
    async def test_successful_token_refresh(self, oauth_client, credentials):
        """Test successful token refresh flow."""
        # Mock successful token response
        token_response = {
            "access_token": "new_access_token_12345",
            "expires_in": 3600,
            "token_type": "Bearer",
        }

        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(200, json=token_response)
        )

        # Refresh token
        access_token = await oauth_client.refresh_access_token(
            refresh_token=credentials["refresh_token"],
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
        )

        # Verify
        assert access_token == "new_access_token_12345"
        assert respx.calls.called

        # Verify request data
        request = respx.calls.last.request
        content = request.content.decode()
        assert "grant_type=refresh_token" in content
        assert f"refresh_token={credentials['refresh_token']}" in content
        assert f"client_id={credentials['client_id']}" in content
        assert f"client_secret={credentials['client_secret']}" in content

    @respx.mock
    async def test_invalid_refresh_token_401(self, oauth_client, credentials):
        """Test handling of invalid refresh token (401 error)."""
        # Mock 401 unauthorized response
        error_response = {
            "error": "invalid_grant",
            "error_description": "Token has been expired or revoked.",
        }

        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(401, json=error_response)
        )

        # Attempt refresh and expect AuthenticationError
        with pytest.raises(AuthenticationError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

        # Verify error details
        assert "Token has been expired or revoked" in str(exc_info.value)
        assert exc_info.value.details["requires_reauth"] is True
        assert not exc_info.value.retriable

    @respx.mock
    async def test_invalid_client_credentials_400(self, oauth_client, credentials):
        """Test handling of invalid client credentials (400 error)."""
        # Mock 400 bad request response
        error_response = {
            "error": "invalid_client",
            "error_description": "The OAuth client was not found.",
        }

        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(400, json=error_response)
        )

        # Attempt refresh and expect AuthenticationError
        with pytest.raises(AuthenticationError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id="invalid_client_id",
                client_secret=credentials["client_secret"],
            )

        # Verify error details
        assert "The OAuth client was not found" in str(exc_info.value)
        assert not exc_info.value.retriable

    @respx.mock
    async def test_rate_limit_429(self, oauth_client, credentials):
        """Test handling of rate limit (429) response."""
        # Mock 429 too many requests
        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(
                429,
                headers={"Retry-After": "30"},
                text="Rate limit exceeded",
            )
        )

        # Attempt refresh and expect RetriableError
        with pytest.raises(RetriableError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

        # Verify error is retriable with retry_after
        assert exc_info.value.retriable
        assert exc_info.value.retry_after == 30
        assert "temporarily unavailable" in str(exc_info.value)

    @respx.mock
    async def test_service_unavailable_503(self, oauth_client, credentials):
        """Test handling of service unavailable (503) response."""
        # Mock 503 service unavailable
        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(
                503,
                headers={"Retry-After": "60"},
                text="Service temporarily unavailable",
            )
        )

        # Attempt refresh and expect RetriableError
        with pytest.raises(RetriableError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

        # Verify error is retriable
        assert exc_info.value.retriable
        assert exc_info.value.retry_after == 60

    @respx.mock
    async def test_server_error_500(self, oauth_client, credentials):
        """Test handling of server error (500) response."""
        # Mock 500 internal server error
        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )

        # Attempt refresh and expect RetriableError
        with pytest.raises(RetriableError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

        # Verify error is retriable
        assert exc_info.value.retriable
        assert "Token endpoint error: 500" in str(exc_info.value)

    @respx.mock
    async def test_network_error(self, oauth_client, credentials):
        """Test handling of network errors."""
        # Mock network error
        respx.post("https://oauth2.googleapis.com/token").mock(
            side_effect=httpx.NetworkError("Connection refused")
        )

        # Attempt refresh and expect RetriableError
        with pytest.raises(RetriableError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

        # Verify error is retriable
        assert exc_info.value.retriable
        assert "Network error" in str(exc_info.value)

    @respx.mock
    async def test_timeout_error(self, oauth_client, credentials):
        """Test handling of timeout errors."""
        # Mock timeout error
        respx.post("https://oauth2.googleapis.com/token").mock(
            side_effect=httpx.TimeoutException("Request timeout")
        )

        # Attempt refresh and expect RetriableError
        with pytest.raises(RetriableError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

        # Verify error is retriable
        assert exc_info.value.retriable
        assert "timed out" in str(exc_info.value)

    @respx.mock
    async def test_missing_access_token_in_response(self, oauth_client, credentials):
        """Test handling of response missing access_token."""
        # Mock response without access_token
        invalid_response = {
            "expires_in": 3600,
            "token_type": "Bearer",
            # Missing access_token
        }

        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(200, json=invalid_response)
        )

        # Attempt refresh and expect AuthenticationError
        with pytest.raises(AuthenticationError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

        # Verify error message
        assert "missing access_token" in str(exc_info.value)

    @respx.mock
    async def test_with_custom_http_client(self, credentials):
        """Test using a custom HTTP client."""
        # Create custom client
        custom_client = httpx.AsyncClient(timeout=10.0)
        oauth_client = GoogleOAuthClient(client=custom_client)

        # Mock successful response
        token_response = {
            "access_token": "custom_client_token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }

        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(200, json=token_response)
        )

        try:
            # Refresh token
            access_token = await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

            # Verify
            assert access_token == "custom_client_token"

        finally:
            # Clean up custom client
            await custom_client.aclose()

    @respx.mock
    async def test_retry_after_header_non_numeric(self, oauth_client, credentials):
        """Test handling of non-numeric Retry-After header."""
        # Mock 429 with non-numeric Retry-After
        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(
                429,
                headers={"Retry-After": "Wed, 21 Oct 2025 07:28:00 GMT"},
                text="Rate limit exceeded",
            )
        )

        # Attempt refresh and expect RetriableError with default retry_after
        with pytest.raises(RetriableError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

        # Verify default retry_after is used
        assert exc_info.value.retry_after == 60

    @respx.mock
    async def test_unexpected_status_code(self, oauth_client, credentials):
        """Test handling of unexpected status codes."""
        # Mock unexpected status code
        respx.post("https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(418, text="I'm a teapot")
        )

        # Attempt refresh and expect AuthenticationError (4xx)
        with pytest.raises(AuthenticationError) as exc_info:
            await oauth_client.refresh_access_token(
                refresh_token=credentials["refresh_token"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
            )

        # Verify error
        assert "status 418" in str(exc_info.value)