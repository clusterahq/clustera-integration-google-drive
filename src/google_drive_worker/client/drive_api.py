"""Google Drive API client with automatic token refresh and rate limiting.

This module provides an async client for interacting with the Google Drive API v3.
It handles authentication, rate limiting, and error classification according to
the integration architecture patterns.
"""

import logging
from typing import Dict, Any, Optional, List
from urllib.parse import urlencode

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .auth import GoogleOAuthClient
from ..utils.errors import (
    RateLimitError,
    RetriableError,
    TerminalError,
    AuthenticationError,
)

logger = logging.getLogger(__name__)


class GoogleDriveAPIClient:
    """Async client for Google Drive API v3.

    Features:
    - Automatic OAuth token refresh on 401/403 errors
    - Rate limit detection with RateLimitError
    - Exponential backoff for retriable errors
    - Proper error classification (retriable vs terminal)
    - Comprehensive structured logging
    """

    BASE_URL = "https://www.googleapis.com/drive/v3"
    DEFAULT_TIMEOUT = 30.0
    MAX_RETRIES = 3

    def __init__(
        self,
        access_token: str,
        refresh_token: str,
        client_id: str,
        client_secret: str,
        oauth_client: Optional[GoogleOAuthClient] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        """Initialize Google Drive API client.

        Args:
            access_token: Current OAuth access token
            refresh_token: OAuth refresh token for token renewal
            client_id: OAuth 2.0 client ID
            client_secret: OAuth 2.0 client secret
            oauth_client: Optional OAuth client for token refresh
            http_client: Optional httpx client for API requests
        """
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._client_id = client_id
        self._client_secret = client_secret
        self._oauth_client = oauth_client or GoogleOAuthClient()
        self._http_client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(self.DEFAULT_TIMEOUT)
        )
        self._token_refreshed = False  # Track if we've refreshed token this session

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - close HTTP client."""
        await self.close()

    async def close(self):
        """Close the HTTP client and clean up resources."""
        if self._http_client:
            await self._http_client.aclose()

    @retry(
        retry=retry_if_exception_type(RetriableError),
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def list_files(
        self,
        page_size: int = 100,
        page_token: Optional[str] = None,
        query: Optional[str] = None,
        fields: Optional[str] = None,
        include_shared_drives: bool = True,
        include_trashed: bool = False,
    ) -> Dict[str, Any]:
        """List files in Google Drive.

        Args:
            page_size: Number of files to return per page (max 1000)
            page_token: Token for retrieving next page of results
            query: Query string for filtering files (Drive API query syntax)
            fields: Comma-separated list of fields to include in response
            include_shared_drives: Whether to include shared drive files
            include_trashed: Whether to include trashed files

        Returns:
            Dictionary containing files list and optional nextPageToken

        Raises:
            RateLimitError: If API rate limit exceeded
            RetriableError: For temporary failures
            TerminalError: For permanent failures
        """
        logger.info(
            "Listing files from Google Drive: page_size=%s, has_page_token=%s, query=%s, include_shared_drives=%s",
            page_size,
            bool(page_token),
            query,
            include_shared_drives,
        )

        params = {
            "pageSize": min(page_size, 1000),
            "supportsAllDrives": include_shared_drives,
            "includeItemsFromAllDrives": include_shared_drives,
        }

        if page_token:
            params["pageToken"] = page_token

        if query:
            # Add trashed filter if needed
            if not include_trashed and "trashed" not in query:
                query = f"({query}) and trashed = false"
            params["q"] = query
        elif not include_trashed:
            params["q"] = "trashed = false"

        if fields:
            params["fields"] = fields
        else:
            # Default fields for file listing
            params["fields"] = "nextPageToken,files(id,name,mimeType,size,createdTime,modifiedTime,parents,webViewLink,owners,permissions,trashed)"

        url = f"{self.BASE_URL}/files"
        response = await self._make_request("GET", url, params=params)

        logger.info(
            "Successfully listed files: file_count=%s, has_next_page=%s",
            len(response.get("files", [])),
            bool(response.get("nextPageToken")),
        )

        return response

    async def get_file(
        self,
        file_id: str,
        fields: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get metadata for a specific file.

        Args:
            file_id: The ID of the file to retrieve
            fields: Comma-separated list of fields to include

        Returns:
            Dictionary containing file metadata

        Raises:
            RateLimitError: If API rate limit exceeded
            RetriableError: For temporary failures
            TerminalError: For permanent failures (including 404)
        """
        logger.info(
            "Getting file metadata from Google Drive: file_id=%s, fields=%s",
            file_id, fields,
        )

        params = {"supportsAllDrives": True}

        if fields:
            params["fields"] = fields
        else:
            # Comprehensive default fields
            params["fields"] = "id,name,mimeType,size,createdTime,modifiedTime,parents,webViewLink,webContentLink,owners,permissions,capabilities,trashed,explicitlyTrashed,md5Checksum,headRevisionId"

        url = f"{self.BASE_URL}/files/{file_id}"
        response = await self._make_request("GET", url, params=params)

        logger.info(
            "Successfully retrieved file metadata: file_id=%s, mime_type=%s, size=%s",
            file_id,
            response.get("mimeType"),
            response.get("size"),
        )

        return response

    async def export_file(
        self,
        file_id: str,
        mime_type: str,
    ) -> bytes:
        """Export a Google Workspace file to a specific format.

        Used for Google Docs, Sheets, Slides, etc. that need to be exported
        to a standard format for processing.

        Args:
            file_id: The ID of the file to export
            mime_type: Target MIME type for export (e.g., 'application/pdf')

        Returns:
            Exported file content as bytes

        Raises:
            RateLimitError: If API rate limit exceeded
            RetriableError: For temporary failures
            TerminalError: For permanent failures (including unsupported export)
        """
        logger.info(
            "Exporting Google Workspace file: file_id=%s, target_mime_type=%s",
            file_id, mime_type,
        )

        params = {"mimeType": mime_type}
        url = f"{self.BASE_URL}/files/{file_id}/export"

        # Export endpoint returns raw bytes, not JSON
        response = await self._make_request(
            "GET", url, params=params, parse_json=False
        )

        logger.info(
            "Successfully exported file: file_id=%s, size_bytes=%s",
            file_id,
            len(response) if isinstance(response, bytes) else 0,
        )

        return response

    async def list_changes(
        self,
        page_token: str,
        page_size: int = 100,
        include_shared_drives: bool = True,
    ) -> Dict[str, Any]:
        """List changes in Google Drive since a given page token.

        Used for incremental sync to detect file changes efficiently.

        Args:
            page_token: The token for fetching changes (from get_start_page_token)
            page_size: Number of changes to return per page
            include_shared_drives: Whether to include shared drive changes

        Returns:
            Dictionary containing changes and optional nextPageToken

        Raises:
            RateLimitError: If API rate limit exceeded
            RetriableError: For temporary failures
            TerminalError: For permanent failures
        """
        logger.info(
            "Listing changes from Google Drive: page_token=%s, page_size=%s, include_shared_drives=%s",
            page_token[:20] + "..." if len(page_token) > 20 else page_token,
            page_size,
            include_shared_drives,
        )

        params = {
            "pageToken": page_token,
            "pageSize": min(page_size, 1000),
            "supportsAllDrives": include_shared_drives,
            "includeItemsFromAllDrives": include_shared_drives,
            "fields": "nextPageToken,newStartPageToken,changes(file(id,name,mimeType,size,createdTime,modifiedTime,parents,webViewLink,owners,permissions,trashed),removed,changeType)",
        }

        url = f"{self.BASE_URL}/changes"
        response = await self._make_request("GET", url, params=params)

        logger.info(
            "Successfully listed changes: change_count=%s, has_next_page=%s, new_start_token=%s",
            len(response.get("changes", [])),
            bool(response.get("nextPageToken")),
            bool(response.get("newStartPageToken")),
        )

        return response

    async def get_start_page_token(
        self,
        supports_all_drives: bool = True,
    ) -> str:
        """Get a page token for starting change tracking.

        Args:
            supports_all_drives: Whether to track changes in all drives

        Returns:
            Page token string for use with list_changes

        Raises:
            RateLimitError: If API rate limit exceeded
            RetriableError: For temporary failures
            TerminalError: For permanent failures
        """
        logger.info(
            "Getting start page token for change tracking: supports_all_drives=%s",
            supports_all_drives,
        )

        params = {"supportsAllDrives": supports_all_drives}
        url = f"{self.BASE_URL}/changes/startPageToken"

        response = await self._make_request("GET", url, params=params)

        start_token = response.get("startPageToken")
        if not start_token:
            raise TerminalError("Failed to get start page token from API")

        logger.info(
            "Successfully retrieved start page token: token_preview=%s",
            start_token[:20] + "..." if len(start_token) > 20 else start_token,
        )

        return start_token

    async def list_revisions(
        self,
        file_id: str,
        page_size: int = 200,
    ) -> List[Dict[str, Any]]:
        """List all revisions for a file.

        Args:
            file_id: The ID of the file
            page_size: Maximum number of revisions to return

        Returns:
            List of revision metadata dictionaries

        Raises:
            RateLimitError: If API rate limit exceeded
            RetriableError: For temporary failures
            TerminalError: For permanent failures
        """
        logger.info(
            "Listing file revisions: file_id=%s, page_size=%s",
            file_id, page_size,
        )

        params = {
            "pageSize": min(page_size, 200),
            "fields": "revisions(id,modifiedTime,lastModifyingUser,size,md5Checksum)",
        }

        url = f"{self.BASE_URL}/files/{file_id}/revisions"
        response = await self._make_request("GET", url, params=params)

        revisions = response.get("revisions", [])

        logger.info(
            "Successfully listed revisions: file_id=%s, revision_count=%s",
            file_id,
            len(revisions),
        )

        return revisions

    async def get_about(
        self,
        fields: str = "user,storageQuota",
    ) -> Dict[str, Any]:
        """Get information about the user's Drive.

        Provides user email and storage quota information.
        Used to validate access during initialization.

        Args:
            fields: Comma-separated fields to include in response

        Returns:
            About resource with user and quota information

        Raises:
            TerminalError: For permanent failures (invalid credentials)
            RetriableError: For temporary failures (network, rate limits)

        Example response:
            {
                "user": {
                    "emailAddress": "user@example.com",
                    "displayName": "John Doe"
                },
                "storageQuota": {
                    "limit": "16106127360",
                    "usage": "1073741824"
                }
            }
        """
        url = f"{self.BASE_URL}/about"
        params = {"fields": fields}

        logger.debug(
            "Fetching Drive about info: fields=%s",
            fields,
        )

        response = await self._make_request("GET", url, params=params)

        logger.info(
            "Successfully fetched Drive about info: email=%s",
            response.get("user", {}).get("emailAddress"),
        )

        return response

    async def watch_changes(
        self,
        page_token: str,
        channel_id: str,
        webhook_url: str,
        channel_token: Optional[str] = None,
        expiration: Optional[int] = None,
        include_shared_drives: bool = True,
    ) -> Dict[str, Any]:
        """Set up push notifications for Drive changes.

        Creates a notification channel that sends HTTPS POST requests to the
        specified webhook URL when files change. Channels expire after a maximum
        of 24 hours and must be renewed.

        Args:
            page_token: Starting page token (from get_start_page_token)
            channel_id: Unique channel ID (UUID recommended)
            webhook_url: HTTPS URL to receive notifications
            channel_token: Optional verification token sent with notifications
            expiration: Optional expiration timestamp in milliseconds (max 24 hours)
            include_shared_drives: Include shared drive items

        Returns:
            Response with channel information:
            {
                "kind": "api#channel",
                "id": "channel_id",
                "resourceId": "resource_id_xyz",
                "resourceUri": "https://www.googleapis.com/drive/v3/changes?pageToken=...",
                "expiration": "1234567890000"
            }

        Raises:
            TerminalError: For permanent failures (invalid webhook URL, bad credentials)
            RetriableError: For temporary failures (network, rate limits)
        """
        url = f"{self.BASE_URL}/changes/watch"

        # Build channel body
        channel_body = {
            "id": channel_id,
            "type": "web_hook",
            "address": webhook_url,
        }

        if channel_token:
            channel_body["token"] = channel_token

        if expiration:
            channel_body["expiration"] = expiration

        # Build query parameters
        params = {
            "pageToken": page_token,
            "supportsAllDrives": include_shared_drives,
            "includeItemsFromAllDrives": include_shared_drives,
        }

        logger.info(
            "Creating Google Drive notification channel: channel_id=%s, webhook_url=%s, expiration=%s",
            channel_id,
            webhook_url,
            expiration,
        )

        response = await self._make_request(
            "POST",
            url,
            params=params,
            json_data=channel_body,
        )

        logger.info(
            "Successfully created notification channel: channel_id=%s, resource_id=%s, expiration=%s",
            response.get("id"),
            response.get("resourceId"),
            response.get("expiration"),
        )

        return response

    async def stop_channel(
        self,
        channel_id: str,
        resource_id: str,
    ) -> None:
        """Stop receiving push notifications for a channel.

        Cancels an active notification channel. After calling this, the webhook
        URL will no longer receive notifications for this channel.

        Args:
            channel_id: The channel ID to stop
            resource_id: The resource ID from watch response

        Raises:
            TerminalError: For permanent failures (channel not found)
            RetriableError: For temporary failures (network, rate limits)
        """
        url = f"{self.BASE_URL}/channels/stop"

        channel_body = {
            "id": channel_id,
            "resourceId": resource_id,
        }

        logger.info(
            "Stopping Google Drive notification channel: channel_id=%s, resource_id=%s",
            channel_id,
            resource_id,
        )

        await self._make_request(
            "POST",
            url,
            json_data=channel_body,
        )

        logger.info(
            "Successfully stopped notification channel: channel_id=%s",
            channel_id,
        )

    async def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        parse_json: bool = True,
    ) -> Any:
        """Make an HTTP request to the Google Drive API.

        Handles authentication, rate limiting, and error classification.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: API endpoint URL
            params: Query parameters
            json_data: JSON body for POST/PATCH requests
            parse_json: Whether to parse response as JSON

        Returns:
            Parsed JSON response or raw bytes if parse_json=False

        Raises:
            RateLimitError: If API rate limit exceeded
            RetriableError: For temporary failures
            TerminalError: For permanent failures
        """
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json" if parse_json else "*/*",
        }

        try:
            response = await self._http_client.request(
                method,
                url,
                params=params,
                json=json_data,
                headers=headers,
            )

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "60")
                logger.warning(
                    "Google Drive API rate limit exceeded: retry_after=%s, url=%s",
                    retry_after,
                    url,
                )
                raise RateLimitError(
                    "Rate limit exceeded",
                    retry_after=int(retry_after) if str(retry_after).isdigit() else 60,
                )

            # Handle authentication errors
            if response.status_code in [401, 403]:
                # Try token refresh once
                if not self._token_refreshed:
                    logger.info("Access token expired, attempting refresh")
                    try:
                        self._access_token = await self._oauth_client.refresh_access_token(
                            self._refresh_token,
                            self._client_id,
                            self._client_secret,
                        )
                        self._token_refreshed = True

                        # Retry request with new token
                        headers["Authorization"] = f"Bearer {self._access_token}"
                        response = await self._http_client.request(
                            method,
                            url,
                            params=params,
                            json=json_data,
                            headers=headers,
                        )

                        # Check if still failing after refresh
                        if response.status_code in [401, 403]:
                            raise AuthenticationError(
                                f"Authentication failed even after token refresh: {response.status_code}"
                            )

                    except AuthenticationError:
                        # Token refresh failed
                        raise
                else:
                    # Already tried refreshing
                    raise AuthenticationError(
                        f"Authentication failed with status {response.status_code}"
                    )

            # Handle server errors (5xx)
            if 500 <= response.status_code < 600:
                logger.error(
                    "Google Drive API server error: status_code=%s, url=%s, response_text=%s",
                    response.status_code,
                    url,
                    response.text[:500] if response.text else None,
                )
                raise RetriableError(
                    f"Google API error: {response.status_code}",
                    details={"status_code": response.status_code},
                )

            # Handle client errors (4xx)
            if 400 <= response.status_code < 500:
                error_detail = self._parse_error_response(response)
                logger.error(
                    "Google Drive API client error: status_code=%s, url=%s, error=%s",
                    response.status_code,
                    url,
                    error_detail,
                )
                raise TerminalError(
                    f"Client error: {response.status_code} - {error_detail.get('message', 'Unknown error')}"
                )

            # Success
            response.raise_for_status()

            if parse_json:
                return response.json()
            else:
                return response.content

        except httpx.NetworkError as e:
            logger.error(
                "Network error calling Google Drive API: url=%s, error=%s",
                url,
                str(e),
            )
            raise RetriableError(
                "Network error calling Google Drive API",
                details={"error": str(e)},
            )
        except httpx.TimeoutException as e:
            logger.error(
                "Timeout calling Google Drive API: url=%s, error=%s",
                url,
                str(e),
            )
            raise RetriableError(
                "Request to Google Drive API timed out",
                details={"error": str(e)},
            )

    def _parse_error_response(self, response: httpx.Response) -> Dict[str, Any]:
        """Parse error response from Google Drive API.

        Args:
            response: HTTP response object

        Returns:
            Parsed error data or raw text
        """
        try:
            error_json = response.json()
            if "error" in error_json:
                return error_json["error"]
            return error_json
        except Exception:
            return {"message": response.text[:500] if response.text else "Unknown error"}