"""Google OAuth 2.0 client for token management.

This module handles OAuth token refresh for Google Drive API authentication.
It follows Google's OAuth 2.0 flow for server-to-server authentication.
"""

import logging
from typing import Dict, Any, Optional
import httpx

from ..utils.errors import AuthenticationError, RetriableError

logger = logging.getLogger(__name__)


class GoogleOAuthClient:
    """OAuth 2.0 client for Google Drive API authentication.

    Handles token refresh using refresh tokens obtained during the initial
    OAuth flow. This client is designed for server-to-server authentication
    where the refresh token is stored securely.
    """

    TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"

    def __init__(self, client: Optional[httpx.AsyncClient] = None):
        """Initialize OAuth client.

        Args:
            client: Optional httpx client for making requests.
                    If not provided, a new client will be created per request.
        """
        self._client = client

    async def refresh_access_token(
        self,
        refresh_token: str,
        client_id: str,
        client_secret: str,
    ) -> str:
        """Refresh an expired access token using a refresh token.

        Args:
            refresh_token: The refresh token obtained during initial OAuth flow
            client_id: OAuth 2.0 client ID from Google Cloud Console
            client_secret: OAuth 2.0 client secret

        Returns:
            New access token string

        Raises:
            AuthenticationError: If refresh token is invalid or revoked
            RetriableError: If network error or temporary API issue
        """
        logger.info(
            "Refreshing Google OAuth access token: client_id=%s, has_refresh_token=%s",
            client_id[:10] + "..." if client_id else None,
            bool(refresh_token),
        )

        # Prepare request data
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }

        # Create client if not provided
        client = self._client or httpx.AsyncClient(timeout=30.0)
        try:
            response = await client.post(
                self.TOKEN_ENDPOINT,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            # Handle response
            if response.status_code == 200:
                token_data = response.json()
                access_token = token_data.get("access_token")

                if not access_token:
                    logger.error(
                        "Token refresh response missing access_token: response_data=%s",
                        token_data,
                    )
                    raise AuthenticationError(
                        "Token refresh response missing access_token"
                    )

                logger.info(
                    "Successfully refreshed access token: expires_in=%s, token_type=%s",
                    token_data.get("expires_in"),
                    token_data.get("token_type", "Bearer"),
                )

                return access_token

            elif response.status_code in [400, 401]:
                # Invalid refresh token or client credentials
                error_data = self._parse_error_response(response)
                error_msg = error_data.get("error_description", "Invalid refresh token")

                logger.error(
                    "Token refresh failed - invalid credentials: status_code=%s, error=%s, error_description=%s",
                    response.status_code,
                    error_data.get("error"),
                    error_msg,
                )

                raise AuthenticationError(
                    f"Token refresh failed: {error_msg}",
                    details={"requires_reauth": True},
                )

            elif response.status_code in [429, 503]:
                # Rate limit or temporary unavailability
                retry_after = response.headers.get("Retry-After", "60")
                logger.warning(
                    "Token refresh temporarily unavailable: status_code=%s, retry_after=%s",
                    response.status_code,
                    retry_after,
                )

                raise RetriableError(
                    f"Token endpoint temporarily unavailable (status {response.status_code})",
                    details={"status_code": response.status_code},
                    retry_after=int(retry_after) if retry_after.isdigit() else 60,
                )

            else:
                # Unexpected error
                error_data = self._parse_error_response(response)
                logger.error(
                    "Unexpected token refresh error: status_code=%s, error_data=%s",
                    response.status_code,
                    error_data,
                )

                # Determine if retriable based on status code
                if response.status_code >= 500:
                    raise RetriableError(
                        f"Token endpoint error: {response.status_code}",
                        details={"status_code": response.status_code, "error": error_data},
                    )
                else:
                    raise AuthenticationError(
                        f"Token refresh failed with status {response.status_code}",
                        details={"requires_reauth": True, "status_code": response.status_code},
                    )

        except httpx.NetworkError as e:
            logger.error(
                "Network error during token refresh: error=%s",
                str(e),
            )
            raise RetriableError(
                "Network error during token refresh",
                details={"error": str(e)},
            )
        except httpx.TimeoutException as e:
            logger.error(
                "Timeout during token refresh: error=%s",
                str(e),
            )
            raise RetriableError(
                "Token refresh request timed out",
                details={"error": str(e)},
            )
        finally:
            # Close client if we created it
            if not self._client:
                await client.aclose()

    def _parse_error_response(self, response: httpx.Response) -> Dict[str, Any]:
        """Parse error response from OAuth endpoint.

        Args:
            response: HTTP response object

        Returns:
            Parsed error data or empty dict
        """
        try:
            return response.json()
        except Exception:
            return {"raw_text": response.text[:500] if response.text else None}