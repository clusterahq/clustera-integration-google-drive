"""Custom exception classes for Google Drive integration worker.

These exceptions help classify errors into retriable and terminal categories,
following the error handling patterns defined in the architecture.
"""

from typing import Optional, Dict, Any


class IntegrationError(Exception):
    """Base exception for all integration errors."""

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        retriable: bool = True,
    ):
        """Initialize integration error.

        Args:
            message: Error message
            details: Additional error details
            retriable: Whether this error is retriable
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.retriable = retriable

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for serialization.

        Returns:
            Dictionary representation of the error
        """
        return {
            "message": self.message,
            "details": self.details,
            "retriable": self.retriable,
            "error_type": self.__class__.__name__,
        }


class RetriableError(IntegrationError):
    """Exception for temporary failures that should be retried.

    Examples:
    - Network timeouts
    - Temporary API unavailability
    - Rate limiting (with backoff)
    """

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        retry_after: Optional[int] = None,
    ):
        """Initialize retriable error.

        Args:
            message: Error message
            details: Additional error details
            retry_after: Seconds to wait before retry (for rate limits)
        """
        super().__init__(message, details, retriable=True)
        self.retry_after = retry_after
        if retry_after:
            self.details["retry_after"] = retry_after


class TerminalError(IntegrationError):
    """Exception for permanent failures that should not be retried.

    Examples:
    - Invalid credentials
    - Resource not found (404)
    - Invalid request format
    - Business logic violations
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """Initialize terminal error.

        Args:
            message: Error message
            details: Additional error details
        """
        super().__init__(message, details, retriable=False)


class RateLimitError(RetriableError):
    """Exception for rate limit errors from Google Drive API.

    Google Drive has quotas:
    - 10,000 queries per 100 seconds per user
    - Per-project quotas
    """

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: Optional[int] = None,
        quota_remaining: Optional[int] = None,
    ):
        """Initialize rate limit error.

        Args:
            message: Error message
            retry_after: Seconds to wait before retry
            quota_remaining: Remaining quota if known
        """
        details = {}
        if quota_remaining is not None:
            details["quota_remaining"] = quota_remaining

        super().__init__(message, details, retry_after=retry_after or 60)


class ValidationError(TerminalError):
    """Exception for validation errors.

    Used when:
    - Required fields are missing
    - Data format is invalid
    - Business rules are violated
    """

    def __init__(self, message: str, field: Optional[str] = None):
        """Initialize validation error.

        Args:
            message: Error message
            field: Field that failed validation
        """
        details = {}
        if field:
            details["field"] = field
        super().__init__(message, details)


class AuthenticationError(TerminalError):
    """Exception for authentication/authorization errors.

    Used when:
    - OAuth tokens are invalid/expired
    - Insufficient permissions
    - Account suspended
    """

    def __init__(
        self,
        message: str = "Authentication failed",
        requires_reauth: bool = True,
    ):
        """Initialize authentication error.

        Args:
            message: Error message
            requires_reauth: Whether user needs to re-authenticate
        """
        super().__init__(message, {"requires_reauth": requires_reauth})