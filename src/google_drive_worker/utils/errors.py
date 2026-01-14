"""Custom exception classes for Google Drive integration worker.

Error classification follows these rules:
- RetriableError: Temporary failures that should be retried with backoff
- TerminalError: Permanent failures that should NOT be retried
"""

from __future__ import annotations

from typing import Any


class IntegrationError(Exception):
    """Base exception for all integration errors."""

    def __init__(
        self,
        message: str,
        *,
        category: str = "unknown",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.category = category
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        """Convert error to dictionary for serialization."""
        return {
            "type": self.__class__.__name__,
            "message": self.message,
            "retriable": isinstance(self, RetriableError),
            "category": self.category,
            "details": self.details,
        }


class RetriableError(IntegrationError):
    """Temporary failure that should be retried with exponential backoff.

    Examples:
    - Network timeouts
    - 5xx server errors
    - Rate limit exceeded (429)
    - Transient service unavailability
    """

    def __init__(
        self,
        message: str,
        *,
        category: str = "transient",
        details: dict[str, Any] | None = None,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(message, category=category, details=details)
        self.retry_after = retry_after


class TerminalError(IntegrationError):
    """Permanent failure that should NOT be retried.

    Examples:
    - Invalid credentials (401)
    - Permission denied (403)
    - Resource not found (404) for non-change endpoints
    - Malformed request (400)
    - Invalid data format
    """

    def __init__(
        self,
        message: str,
        *,
        category: str = "permanent",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, category=category, details=details)


class RateLimitError(RetriableError):
    """Rate limit exceeded - should retry after backoff.

    Typically 429 Too Many Requests responses.
    Google Drive quotas: 12,000 queries per minute per user.
    """

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        *,
        retry_after: int | None = None,
        remaining_quota: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if remaining_quota is not None:
            details["remaining_quota"] = remaining_quota
        super().__init__(
            message,
            category="rate_limit",
            details=details,
            retry_after=retry_after,
        )


class ValidationError(TerminalError):
    """Message validation failed - malformed or missing required fields."""

    def __init__(
        self,
        message: str,
        *,
        field: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if field:
            details["field"] = field
        super().__init__(message, category="validation", details=details)


class AuthenticationError(TerminalError):
    """Authentication failed - invalid or expired credentials.

    Typically 401 Unauthorized responses.
    """

    def __init__(
        self,
        message: str = "Authentication failed",
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, category="authentication", details=details)


class AuthorizationError(TerminalError):
    """Authorization failed - insufficient permissions.

    Typically 403 Forbidden responses.
    """

    def __init__(
        self,
        message: str = "Authorization failed",
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, category="authorization", details=details)


class ConfigurationError(TerminalError):
    """Configuration error - missing or invalid configuration."""

    def __init__(
        self,
        message: str,
        *,
        config_key: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if config_key:
            details["config_key"] = config_key
        super().__init__(message, category="configuration", details=details)


class ResourceNotFoundError(TerminalError):
    """Resource not found in provider API.

    Typically 404 Not Found responses (except for changes endpoint).
    """

    def __init__(
        self,
        message: str = "Resource not found",
        *,
        resource_type: str | None = None,
        resource_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if resource_type:
            details["resource_type"] = resource_type
        if resource_id:
            details["resource_id"] = resource_id
        super().__init__(message, category="not_found", details=details)


class ChangeTokenExpiredError(TerminalError):
    """Google Drive change token (pageToken) has expired - requires full re-sync.

    This is a special case of 404 from the changes.list endpoint.
    When this occurs, the worker should trigger a full_sync instead.
    """

    def __init__(
        self,
        message: str = "Change token expired, full sync required",
        *,
        page_token: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if page_token:
            details["expired_page_token"] = page_token
        details["requires_backfill"] = True
        super().__init__(message, category="change_token_expired", details=details)