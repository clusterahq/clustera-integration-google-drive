"""Base handler abstraction for Google Drive worker.

Defines the contract that all handlers must implement.
Inherits from toolkit's BaseActionHandler to get:
- Connection credentials fetching (fetch_connection_config)
- Control Plane client management
- Simplified request resolution (resolve_resource_type, resolve_limit, etc.)
- Pagination helpers
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import AsyncGenerator
from typing import Any, Optional

import structlog
from clustera_integration_toolkit.handlers import (
    BaseActionHandler as ToolkitBaseActionHandler,
)


class BaseActionHandler(ToolkitBaseActionHandler):
    """Google Drive-specific base handler extending toolkit's BaseActionHandler.

    Inherits from toolkit's BaseActionHandler to get:
    - fetch_connection_config(): Fetch OAuth credentials from Control Plane
    - control_plane_client: Lazy-initialized Control Plane client
    - close(): Clean up resources
    - resolve_resource_type/limit/cursor/filters/options(): Simplified request handling
    - create_simplified_response/create_error_response(): Response builders
    - to_provider_pagination(): Pagination format conversion

    Adds Google Drive-specific functionality:
    - SUPPORTED_ACTIONS routing
    - JSON-RPC 2.0 message parsing helpers
    - Google Drive-specific idempotency key pattern
    """

    # Provider identification (inherited from toolkit, override values)
    PROVIDER = "google-drive"
    INTEGRATION_ID = "google-drive"
    DEFAULT_RESOURCE_TYPE = "files"

    # Default filters for simplified requests (inherited from toolkit)
    DEFAULT_FILTERS: dict[str, dict[str, Any]] = {
        "files": {"trashed": False},
        "folders": {"trashed": False, "mimeType": "application/vnd.google-apps.folder"},
    }

    # Override in subclass to specify which action(s) this handler processes
    SUPPORTED_ACTIONS: set[str] = set()

    def __init__(self) -> None:
        """Initialize handler.

        Note: Subclasses may pass additional config (e.g., api_config) if needed.
        """
        super().__init__()  # Initialize toolkit base (Control Plane client, etc.)
        self.logger = structlog.get_logger().bind(handler=self.__class__.__name__)

    def can_handle(self, message: dict[str, Any]) -> bool:
        """Determine if this handler can process the given message.

        Routes based on action field in the message.

        Args:
            message: The message payload

        Returns:
            True if this handler should process the message
        """
        action = message.get("action")
        integration_id = message.get("integration_id")

        # Must be a google-drive message with a supported action
        if integration_id != self.INTEGRATION_ID:
            return False

        return action in self.SUPPORTED_ACTIONS

    @abstractmethod
    async def process_message(
        self,
        message: dict[str, Any],
        connection_config: dict[str, Any],
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Process a message and yield output records.

        Args:
            message: The input message
            connection_config: Connection configuration including credentials

        Yields:
            Records for integrations-incoming-records topic
        """
        ...

    def generate_idempotency_key(
        self,
        connection_id: str,
        resource_type: str,
        resource_id: str,
    ) -> str:
        """Generate a deterministic idempotency key.

        Pattern: google-drive:{connection_id}:{resource_type}:{resource_id}

        Args:
            connection_id: Integration connection ID
            resource_type: Type of resource (file, folder, permission, revision)
            resource_id: Provider's unique ID for the resource

        Returns:
            Deterministic idempotency key
        """
        return f"{self.PROVIDER}:{connection_id}:{resource_type}:{resource_id}"

    # =========================================================================
    # JSON-RPC 2.0 message parsing helpers
    # =========================================================================

    def is_jsonrpc_request(self, message: dict[str, Any]) -> bool:
        """Check if the message is a JSON-RPC 2.0 request.

        Args:
            message: The incoming message dict

        Returns:
            True if JSON-RPC 2.0 format, False otherwise
        """
        return message.get("jsonrpc") == "2.0" and "method" in message

    def extract_params(self, message: dict[str, Any]) -> dict[str, Any]:
        """Extract params from JSON-RPC 2.0 message or return message for legacy format.

        Args:
            message: The incoming message dict

        Returns:
            Params dict (from JSON-RPC params field or the message itself)
        """
        if self.is_jsonrpc_request(message):
            return message.get("params", {})
        return message

    def extract_header_params(self, message: dict[str, Any]) -> dict[str, Any]:
        """Extract header parameters from JSON-RPC 2.0 message.

        Args:
            message: The incoming message dict

        Returns:
            Header parameters dict
        """
        params = self.extract_params(message)
        header = params.get("header", {})
        return header.get("parameters", {})

    # NOTE: fetch_connection_config() is inherited from ToolkitBaseActionHandler
    # It fetches OAuth credentials from Control Plane using M2M authentication.
    # See: clustera_integration_toolkit.handlers.base.BaseActionHandler

    # NOTE: create_response_envelope() is inherited from ToolkitBaseActionHandler
    # It automatically includes DATAPLANE_M2M_TOKEN for authentication

    # NOTE: create_simplified_response() and create_error_response() are also
    # inherited from ToolkitBaseActionHandler for standardized response building

# Legacy alias for backward compatibility
BaseIntegrationHandler = BaseActionHandler
