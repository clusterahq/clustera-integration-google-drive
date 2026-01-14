"""Fetch handler for Google Drive worker.

Exposes Google Drive API capabilities as generic fetch operations using JSON-RPC 2.0 format.
The worker does NOT track cursors - pagination tokens are passed in requests
and returned in responses for the external system to manage.

Handles clustera.integration.content.fetch requests per message-dictionary spec.

For files, emits one clustera.integration.content.ingest message per file with
the file metadata as text content and optionally file content as file reference to S3.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any, Literal

from clustera_integration_toolkit.clusterspace import build_snowball_clusterspace
from clustera_integration_toolkit.message import RecordSubmitBuilder

from google_drive_worker.client.drive_api import GoogleDriveAPIClient
from google_drive_worker.config import GoogleDriveAPIConfig, StorageConfig
from google_drive_worker.handlers.base import BaseGoogleDriveHandler
from google_drive_worker.handlers.content_emitter import ContentIngestEmitter
from google_drive_worker.normalization.transformer import GoogleDriveDataTransformer
from google_drive_worker.schemas.fetch import (
    FetchFilters,
    FetchOptions,
    FetchPagination,
    parse_fetch_params,
)
from google_drive_worker.utils.errors import TerminalError, ValidationError

# Supported resource types for fetch operations
FetchResourceType = Literal[
    "files",        # List files with pagination
    "file",         # Single file by ID
    "folders",      # List folders with pagination
    "folder",       # Single folder by ID
    "permissions",  # List permissions for a file
    "permission",   # Single permission by ID
    "revisions",    # List revisions for a file
    "revision",     # Single revision by ID
    "changes",      # Changes list for incremental sync
    "about",        # User info and Drive metadata
]


class FetchHandler(BaseGoogleDriveHandler):
    """Handler for fetch requests (JSON-RPC 2.0 format).

    Fetches Google Drive resources based on resource_type, filters, and pagination.
    Returns fetched resources with pagination cursors for the external system.

    Handles:
    - JSON-RPC 2.0: method "clustera.integration.content.fetch"
    - Legacy: action "fetch" (for backward compatibility)

    The worker does NOT track pagination state internally. Cursors are:
    - Passed in via params.pagination.page_token
    - Returned in response via payload.pagination

    The external system is responsible for:
    - Storing cursors between requests
    - Deciding when to fetch next page
    - Managing backfill orchestration
    """

    # JSON-RPC 2.0 method routing
    SUPPORTED_METHODS = {"clustera.integration.content.fetch"}

    # Legacy action routing (backward compatibility)
    SUPPORTED_ACTIONS = {"fetch"}

    SUPPORTED_RESOURCE_TYPES: set[FetchResourceType] = {
        "files",
        "file",
        "folders",
        "folder",
        "permissions",
        "permission",
        "revisions",
        "revision",
        "changes",
        "about",
    }

    DEFAULT_LIMIT: int = 100
    MAX_LIMIT: int = 1000

    def __init__(
        self,
        api_config: GoogleDriveAPIConfig,
        storage_config: StorageConfig | None = None,
    ) -> None:
        """Initialize fetch handler.

        Args:
            api_config: Google Drive API configuration
            storage_config: Storage configuration (for file uploads)
        """
        super().__init__(api_config)
        self.transformer = GoogleDriveDataTransformer()
        self.storage_config = storage_config or StorageConfig()
        # Use ContentIngestEmitter for building content.ingest messages
        self._content_emitter: ContentIngestEmitter | None = None

    @property
    def content_emitter(self) -> ContentIngestEmitter:
        """Lazy-initialize content ingest emitter."""
        if self._content_emitter is None:
            self._content_emitter = ContentIngestEmitter(
                api_config=self.api_config,
                storage_config=self.storage_config,
                logger=self.logger,
            )
        return self._content_emitter

    def _build_empty_batch_response(
        self,
        customer_id: str,
        connection_id: str,
        connection_config: dict[str, Any],
        resource_type: str,
        next_page_token: str | None,
        batch_id: str,
        start_time: datetime,
    ) -> dict[str, Any]:
        """Build content.ingest message for empty batch with pagination info.

        Args:
            customer_id: Customer ID
            connection_id: Connection ID
            connection_config: Connection config with snowball_clusterspace
            resource_type: Type of resource being fetched
            next_page_token: Next page token if more pages exist
            batch_id: Batch UUID for correlation
            start_time: Operation start time

        Returns:
            clustera.integration.content.ingest message dict
        """
        processing_duration_ms = int(
            (datetime.now(UTC) - start_time).total_seconds() * 1000
        )

        # Build clusterspace from connection config
        clusterspace = connection_config.get("snowball_clusterspace")
        if not clusterspace:
            # Fallback: build from customer_id and snowball_id
            snowball_id = connection_config.get("snowball_id", "")
            clusterspace = build_snowball_clusterspace(customer_id, snowball_id)

        return RecordSubmitBuilder.build(
            customer_id=customer_id,
            integration_provider_name="google-drive",
            integration_connection_id=connection_id,
            clusterspace=clusterspace,
            resource_type=f"empty_batch_{resource_type}",
            resource_id=batch_id,
            contents=[
                RecordSubmitBuilder.text_content(
                    json.dumps({"empty": True, "resource_type": resource_type})
                )
            ],
            metadata={
                "empty_batch": True,
                "original_resource_type": resource_type,
                "processing_duration_ms": processing_duration_ms,
                "pagination": {
                    "next_page_token": next_page_token,
                    "has_more": next_page_token is not None,
                },
            },
            correlation_id=f"google-drive:{connection_id}:{resource_type}:empty:{batch_id}",
            batch_context={
                "batch_id": batch_id,
                "batch_sequence": 0,
                "batch_is_last": next_page_token is None,
                "batch_page_token": next_page_token,
            },
        )

    async def process_message(
        self,
        message: dict[str, Any],
        connection_config: dict[str, Any],
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Process a fetch message (JSON-RPC 2.0 or legacy format).

        JSON-RPC 2.0 format:
        {
            "id": "request-id",
            "jsonrpc": "2.0",
            "method": "clustera.integration.content.fetch",
            "params": {
                "header": {...},
                "resource_type": "files",
                "filters": {...},
                "pagination": {"max_results": 100, "page_token": null}
            }
        }

        Args:
            message: The fetch message
            connection_config: Connection configuration with credentials

        Yields:
            Response envelope with fetched resources and pagination
        """
        self._validate_message(message)

        # Extract params - handles both JSON-RPC 2.0 and legacy formats
        params = self.extract_params(message)
        header_params = self.extract_header_params(message)

        # Get connection info from header params (JSON-RPC) or message root (legacy)
        connection_id = header_params.get(
            "integration_connection_id",
            message.get("integration_connection_id")
        )
        # Use customer_id from Control Plane credentials (authoritative),
        # falling back to message for backward compatibility
        customer_id = connection_config.get("customer_id") or header_params.get(
            "customer_id", message.get("customer_id")
        )
        trace_id = header_params.get("trace_id")

        # Extract resource_type (required in JSON-RPC 2.0 format)
        resource_type: FetchResourceType = params.get("resource_type", "files")  # type: ignore

        # Extract pagination from params
        pagination_params = params.get("pagination", {})
        limit = min(
            max(1, pagination_params.get("max_results", self.DEFAULT_LIMIT)),
            self.MAX_LIMIT
        )
        cursor = pagination_params.get("page_token")

        # Get filters and options from params
        raw_filters = params.get("filters", {})
        raw_options = params.get("options", params.get("fetch_options", {}))

        # Parse filters, pagination, and options
        filters, pagination, options = parse_fetch_params(
            raw_filters,
            {"page_token": cursor, "max_results": limit},
            raw_options,
        )

        is_jsonrpc = self.is_jsonrpc_request(message)
        self.logger.info(
            "Processing fetch request",
            connection_id=connection_id,
            resource_type=resource_type,
            page_token=pagination.page_token,
            max_results=pagination.max_results,
            jsonrpc_format=is_jsonrpc,
        )

        access_token = connection_config.get("access_token")
        refresh_token = connection_config.get("refresh_token")

        # TODO: Initialize Google Drive API client
        # api_client = GoogleDriveAPIClient(
        #     config=self.api_config,
        #     access_token=access_token,
        #     logger=self.logger,
        #     refresh_token=refresh_token,
        # )

        start_time = datetime.now(UTC)

        try:
            # Route to appropriate fetch method
            if resource_type == "files":
                # TODO: Emit one content.ingest message per file
                # async for response in self._fetch_files_record_submit(...):
                #     yield response
                pass

            elif resource_type == "file":
                # TODO: Single file fetch
                # async for response in self._fetch_file(...):
                #     yield response
                pass

            elif resource_type == "folders":
                # TODO: Emit one content.ingest message per folder
                # async for response in self._fetch_folders(...):
                #     yield response
                pass

            elif resource_type == "folder":
                # TODO: Single folder fetch
                # async for response in self._fetch_folder(...):
                #     yield response
                pass

            elif resource_type == "permissions":
                # TODO: List permissions for a file
                # async for response in self._fetch_permissions(...):
                #     yield response
                pass

            elif resource_type == "permission":
                # TODO: Single permission fetch
                # async for response in self._fetch_permission(...):
                #     yield response
                pass

            elif resource_type == "revisions":
                # TODO: List revisions for a file
                # async for response in self._fetch_revisions(...):
                #     yield response
                pass

            elif resource_type == "revision":
                # TODO: Single revision fetch
                # async for response in self._fetch_revision(...):
                #     yield response
                pass

            elif resource_type == "changes":
                # TODO: Incremental sync via Changes API
                # async for response in self._fetch_changes(...):
                #     yield response
                pass

            elif resource_type == "about":
                # TODO: User info and Drive metadata
                # async for response in self._fetch_about(...):
                #     yield response
                pass

        finally:
            # await api_client.close()
            pass

    def _validate_message(self, message: dict[str, Any]) -> None:
        """Validate required fields in fetch message.

        Supports both JSON-RPC 2.0 and legacy formats:
        - JSON-RPC: method "clustera.integration.content.fetch", params.resource_type required
        - Legacy: action "fetch", resource_type in message root

        Args:
            message: The message to validate

        Raises:
            ValidationError: If required fields are missing or invalid
        """
        # JSON-RPC 2.0 format validation
        if self.is_jsonrpc_request(message):
            if message.get("method") != "clustera.integration.content.fetch":
                raise ValidationError(
                    f"Invalid method for FetchHandler: {message.get('method')}",
                    field="method",
                )

            params = message.get("params", {})
            header = params.get("header", {})
            header_params = header.get("parameters", {})

            # Validate required header parameters
            if not header_params.get("customer_id"):
                raise ValidationError(
                    "Missing required field: params.header.parameters.customer_id",
                    field="customer_id",
                )
            if not header_params.get("integration_connection_id"):
                raise ValidationError(
                    "Missing required field: params.header.parameters.integration_connection_id",
                    field="integration_connection_id",
                )

            # resource_type uses default if not specified
            resource_type = params.get("resource_type") or self.DEFAULT_RESOURCE_TYPE
            if resource_type not in self.SUPPORTED_RESOURCE_TYPES:
                raise ValidationError(
                    f"Unsupported resource type: {resource_type}",
                    field="resource_type",
                    details={"supported": list(self.SUPPORTED_RESOURCE_TYPES)},
                )
            return

        # Legacy format validation
        required_fields = [
            "message_id",
            "customer_id",
            "integration_id",
            "integration_connection_id",
            "action",
        ]

        for field in required_fields:
            if field not in message:
                raise ValidationError(
                    f"Missing required field: {field}",
                    field=field,
                )

        if message.get("action") != "fetch":
            raise ValidationError(
                f"Invalid action for FetchHandler: {message.get('action')}",
                field="action",
            )

        # Validate resource_type if provided
        resource_type = message.get("resource_type")
        if resource_type is not None and resource_type not in self.SUPPORTED_RESOURCE_TYPES:
            raise ValidationError(
                f"Unsupported resource type: {resource_type}",
                field="resource_type",
                details={"supported": list(self.SUPPORTED_RESOURCE_TYPES)},
            )

    # TODO: Implement fetch methods for each resource type
    # - _fetch_files_record_submit (list files, emit content.ingest per file)
    # - _fetch_file (single file by ID, emit content.ingest)
    # - _fetch_folders (list folders, emit content.ingest per folder)
    # - _fetch_folder (single folder by ID, emit content.ingest)
    # - _fetch_permissions (list permissions for a file, emit incoming)
    # - _fetch_permission (single permission by ID, emit incoming)
    # - _fetch_revisions (list revisions for a file, emit incoming)
    # - _fetch_revision (single revision by ID, emit incoming)
    # - _fetch_changes (incremental sync via Changes API, emit content.ingest/incoming)
    # - _fetch_about (user info and Drive metadata, emit incoming)
