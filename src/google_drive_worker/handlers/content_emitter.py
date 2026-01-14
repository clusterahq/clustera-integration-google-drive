"""Content ingest message emitter for Google Drive worker.

Shared module for building clustera.integration.content.ingest messages from Google
Drive file data. Used by both fetch operations (backfill) and webhook handlers (real-time).

This module encapsulates the logic to:
1. Process Google Drive files (metadata extraction, export format handling)
2. Build content.ingest message envelopes with text and file contents
3. Extract file metadata for structured indexing
4. Handle different file types (Google Workspace files, native files, folders)

All methods maintain the exact same message format for consistency across
backfill and webhook ingestion paths.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog
from clustera_integration_toolkit import build_clusterspace_path
from clustera_integration_toolkit.message import RecordSubmitBuilder

from google_drive_worker.config import GoogleDriveAPIConfig, StorageConfig
from google_drive_worker.normalization.transformer import GoogleDriveDataTransformer
from google_drive_worker.normalization.mime_types import (
    is_folder,
    is_google_workspace_file,
    needs_export,
)


def generate_idempotency_key(
    provider: str,
    connection_id: str,
    resource_type: str,
    resource_id: str,
) -> str:
    """Generate a deterministic idempotency key.

    Pattern: {provider}:{connection_id}:{resource_type}:{resource_id}

    Args:
        provider: Integration provider name (e.g., "google-drive")
        connection_id: Integration connection ID
        resource_type: Type of resource (file, folder, permission, revision)
        resource_id: Provider's unique ID for the resource

    Returns:
        Deterministic idempotency key

    Examples:
        >>> generate_idempotency_key("google-drive", "conn_abc", "file", "1A2B3C4D5E")
        "google-drive:conn_abc:file:1A2B3C4D5E"
    """
    return f"{provider}:{connection_id}:{resource_type}:{resource_id}"


class ContentIngestEmitter:
    """Builds clustera.integration.content.ingest messages from Google Drive file data.

    This class encapsulates all logic for converting Google Drive API file data into
    the content.ingest format used by the Data Plane. It handles:

    - File metadata extraction and normalization
    - Text content formatting for different file types
    - Google Workspace file export format handling
    - Folder vs file type detection
    - Message envelope construction with batch context

    Used by:
    - Fetch operations: Backfill file lists (paginated)
    - Webhook handlers: Real-time file change notifications

    Both paths produce identical message formats for consistency.
    """

    def __init__(
        self,
        api_config: GoogleDriveAPIConfig,
        storage_config: StorageConfig | None = None,
        logger: structlog.BoundLogger | None = None,
    ) -> None:
        """Initialize content ingest emitter.

        Args:
            api_config: Google Drive API configuration
            storage_config: Storage configuration (for S3 uploads, future enhancement)
            logger: Structured logger instance (optional, creates new if not provided)
        """
        self.api_config = api_config
        self.storage_config = storage_config or StorageConfig()
        self.logger = logger or structlog.get_logger().bind(
            component="ContentIngestEmitter"
        )
        self.transformer = GoogleDriveDataTransformer()

    async def process_file_for_ingest(
        self,
        file_data: dict[str, Any],
        connection_config: dict[str, Any],
        customer_id: str,
        snowball_id: str,
        batch_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Process a single Google Drive file and build content.ingest envelope.

        This is the main entry point for converting a Google Drive file into a
        content.ingest message. It orchestrates:

        1. File metadata extraction and normalization
        2. Message envelope construction
        3. Content type detection and formatting

        Args:
            file_data: Full file data from Google Drive API (files.get or files.list)
            connection_config: Connection configuration dict
            customer_id: Customer UUID
            snowball_id: Target snowball UUID
            batch_context: Optional batch metadata (batch_id, batch_sequence, batch_is_last, batch_page_token)

        Returns:
            Complete clustera.integration.content.ingest message dict

        Example:
            >>> emitter = ContentIngestEmitter(api_config, storage_config)
            >>> msg = await emitter.process_file_for_ingest(
            ...     file_data=drive_file_data,
            ...     connection_config={"integration_connection_id": "conn_abc"},
            ...     customer_id="cust_xyz",
            ...     snowball_id="snow_123",
            ...     batch_context={"batch_id": "batch_001", "batch_sequence": 0, "batch_is_last": False},
            ... )
        """
        # Build content.ingest message envelope
        return self.build_record_submit_message(
            file_data=file_data,
            connection_config=connection_config,
            customer_id=customer_id,
            snowball_id=snowball_id,
            batch_context=batch_context or {},
        )

    def build_record_submit_message(
        self,
        file_data: dict[str, Any],
        connection_config: dict[str, Any],
        customer_id: str,
        snowball_id: str,
        batch_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Build clustera.integration.content.ingest message for a single file.

        Constructs the complete message envelope including:
        - File metadata as text content (formatted description)
        - Structured metadata for indexing
        - Batch context for pagination tracking
        - Idempotency key for deduplication

        Args:
            file_data: Full file data from Google Drive API
            connection_config: Connection configuration
            customer_id: Customer UUID
            snowball_id: Target snowball UUID
            batch_context: Batch metadata (batch_id, batch_sequence, batch_is_last, batch_page_token)

        Returns:
            Complete clustera.integration.content.ingest message dict

        Example output structure:
            {
                "id": "<uuid>",
                "jsonrpc": "2.0",
                "method": "clustera.integration.content.ingest",
                "params": {
                    "header": {
                        "to_addresses": [{"topics": ["integrations-incoming-records"], "topic_key": "conn_abc"}],
                        "return_addresses": [],
                        "message_call_stack": [],
                        "parameters": {
                            "customer_id": "cust_xyz",
                            "integration_provider_name": "google-drive",
                            "integration_connection_id": "conn_abc",
                            "clusterspace": "/<customer>/graph/snowball/<graph>",
                            "correlation_id": "google-drive:conn_abc:file:1A2B3C4D5E",
                            "energy": 5,
                            "batch_id": "...",
                            "batch_sequence": 0,
                            "batch_is_last": false
                        }
                    },
                    "contents": [
                        {"type": "text", "text": "File: Q4 Report.docx\\nType: Google Docs..."}
                    ],
                    "metadata": {
                        "resource_type": "file",
                        "resource_id": "1A2B3C4D5E",
                        "name": "Q4 Report.docx",
                        "mime_type": "application/vnd.google-apps.document",
                        ...
                    }
                }
            }
        """
        # Get connection ID
        connection_id = connection_config.get("integration_connection_id", "unknown")

        # Transform to normalized format for metadata extraction
        normalized = self.transformer.transform_file(
            raw_file=file_data,
            connection_id=connection_id,
            customer_id=customer_id,
        )

        # Build contents array
        contents = []

        # Add file metadata as text content
        file_text = self._build_content(normalized)
        contents.append(RecordSubmitBuilder.text_content(file_text))

        # Build clusterspace path
        clusterspace = build_clusterspace_path(
            customer_id=customer_id,
            graph_type="snowball",
            graph_id=snowball_id,
        )

        # Resource identification
        resource_type = "folder" if normalized.get("is_folder") else "file"
        resource_id = file_data["id"]

        # Generate deterministic idempotency key
        correlation_id = self._generate_idempotency_key(
            connection_id=connection_id,
            resource_type=resource_type,
            resource_id=resource_id,
        )

        # Extract file metadata for structured data access
        metadata = self._extract_metadata(normalized)

        # Build clustera.integration.content.ingest message using RecordSubmitBuilder
        return RecordSubmitBuilder.build(
            customer_id=customer_id,
            integration_provider_name="google-drive",
            integration_connection_id=connection_id,
            clusterspace=clusterspace,
            resource_type=resource_type,
            resource_id=resource_id,
            contents=contents,
            metadata=metadata,
            correlation_id=correlation_id,
            energy=5,
            batch_context=batch_context,
        )

    def _build_content(self, normalized: dict[str, Any]) -> str:
        """Extract text content from normalized file data.

        Builds a text representation of the file metadata suitable for content ingestion.
        This text will be indexed and searchable by downstream consumers.

        Args:
            normalized: Normalized file data from transformer

        Returns:
            Formatted text representation of file metadata

        Example:
            >>> emitter = ContentIngestEmitter(api_config)
            >>> normalized = {
            ...     "name": "Q4 Report.docx",
            ...     "mime_type": "application/vnd.google-apps.document",
            ...     "category": "document",
            ...     "is_google_workspace": True,
            ...     "size_bytes": 12345,
            ...     "web_view_link": "https://drive.google.com/...",
            ... }
            >>> emitter._build_content(normalized)
            "File: Q4 Report.docx\\nType: Google Docs (document)\\nSize: 12345 bytes\\nLink: https://..."
        """
        parts = []

        # File name
        name = normalized.get("name", "Untitled")
        parts.append(f"File: {name}")

        # File type description
        if normalized.get("is_folder"):
            parts.append("Type: Folder")
        elif normalized.get("is_google_workspace"):
            category = normalized.get("category", "file")
            workspace_type = self._get_workspace_type_name(normalized.get("mime_type", ""))
            parts.append(f"Type: {workspace_type} ({category})")
        else:
            category = normalized.get("category", "file")
            parts.append(f"Type: {category}")

        # File size (if available, not available for folders or Google Workspace files)
        if normalized.get("size_bytes") is not None:
            size_kb = normalized["size_bytes"] / 1024
            if size_kb > 1024:
                size_str = f"{size_kb / 1024:.2f} MB"
            else:
                size_str = f"{size_kb:.2f} KB"
            parts.append(f"Size: {size_str}")

        # Web view link
        if normalized.get("web_view_link"):
            parts.append(f"Link: {normalized['web_view_link']}")

        # Parent folders
        if normalized.get("parent_ids"):
            parent_count = len(normalized["parent_ids"])
            parts.append(f"Parent folders: {parent_count}")

        # Owners
        if normalized.get("owners"):
            owner_names = [owner.get("name", owner.get("email", "Unknown"))
                          for owner in normalized["owners"]]
            parts.append(f"Owners: {', '.join(owner_names)}")

        # Timestamps
        if normalized.get("timestamps"):
            timestamps = normalized["timestamps"]
            if timestamps.get("created_at"):
                parts.append(f"Created: {timestamps['created_at']}")
            if timestamps.get("modified_at"):
                parts.append(f"Modified: {timestamps['modified_at']}")

        # Status flags
        status_flags = []
        if normalized.get("trashed"):
            status_flags.append("trashed")
        if normalized.get("starred"):
            status_flags.append("starred")
        if normalized.get("shared"):
            status_flags.append("shared")
        if status_flags:
            parts.append(f"Status: {', '.join(status_flags)}")

        return "\n".join(parts)

    def _generate_idempotency_key(
        self,
        connection_id: str,
        resource_type: str,
        resource_id: str,
    ) -> str:
        """Generate deterministic idempotency key for file.

        Args:
            connection_id: Integration connection ID
            resource_type: Resource type (file, folder)
            resource_id: Google Drive file ID

        Returns:
            Deterministic idempotency key

        Examples:
            >>> emitter._generate_idempotency_key("conn_abc", "file", "1A2B3C4D5E")
            "google-drive:conn_abc:file:1A2B3C4D5E"
        """
        return generate_idempotency_key(
            provider="google-drive",
            connection_id=connection_id,
            resource_type=resource_type,
            resource_id=resource_id,
        )

    def _extract_metadata(self, normalized: dict[str, Any]) -> dict[str, Any]:
        """Extract structured file metadata from normalized data.

        Extracts all relevant file fields as structured data that can be
        queried and indexed by downstream consumers.

        Args:
            normalized: Normalized file data from transformer

        Returns:
            File metadata dict with fields:
                - drive_id: Google Drive file ID
                - name: File name
                - mime_type: MIME type
                - category: File category (document, spreadsheet, image, etc.)
                - is_folder: Whether this is a folder
                - is_google_workspace: Whether this is a Google Workspace file
                - export_format: Export MIME type for Google Workspace files (if applicable)
                - size_bytes: File size in bytes (if available)
                - web_view_link: Google Drive web view URL
                - thumbnail_link: Thumbnail URL (if available)
                - parent_ids: List of parent folder IDs
                - owners: List of owner objects with name and email
                - permissions: List of permission objects (if available)
                - trashed: Whether file is trashed
                - starred: Whether file is starred
                - shared: Whether file is shared
                - created_at: Creation timestamp (ISO 8601)
                - modified_at: Last modification timestamp (ISO 8601)
                - viewed_by_me_time: Last viewed timestamp (ISO 8601, if available)

        Example output:
            {
                "drive_id": "1A2B3C4D5E",
                "name": "Q4 Report.docx",
                "mime_type": "application/vnd.google-apps.document",
                "category": "document",
                "is_folder": false,
                "is_google_workspace": true,
                "export_format": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "web_view_link": "https://drive.google.com/...",
                "parent_ids": ["0B1C2D3E4F"],
                "owners": [{"name": "Alice", "email": "alice@example.com"}],
                "trashed": false,
                "starred": false,
                "shared": true,
                "created_at": "2025-01-15T10:00:00Z",
                "modified_at": "2025-01-20T15:30:00Z"
            }
        """
        metadata: dict[str, Any] = {
            "drive_id": normalized.get("id"),
            "name": normalized.get("name"),
            "mime_type": normalized.get("mime_type"),
            "category": normalized.get("category"),
            "is_folder": normalized.get("is_folder", False),
            "is_google_workspace": normalized.get("is_google_workspace", False),
            "trashed": normalized.get("trashed", False),
            "starred": normalized.get("starred", False),
        }

        # Add optional fields only if present
        if normalized.get("export_format"):
            metadata["export_format"] = normalized["export_format"]

        if normalized.get("size_bytes") is not None:
            metadata["size_bytes"] = normalized["size_bytes"]

        if normalized.get("web_view_link"):
            metadata["web_view_link"] = normalized["web_view_link"]

        if normalized.get("thumbnail_link"):
            metadata["thumbnail_link"] = normalized["thumbnail_link"]

        if normalized.get("parent_ids"):
            metadata["parent_ids"] = normalized["parent_ids"]

        if normalized.get("owners"):
            metadata["owners"] = normalized["owners"]

        if normalized.get("permissions"):
            metadata["permissions"] = normalized["permissions"]

        if normalized.get("shared") is not None:
            metadata["shared"] = normalized["shared"]

        # Timestamps
        if normalized.get("timestamps"):
            timestamps = normalized["timestamps"]
            if timestamps.get("created_at"):
                metadata["created_at"] = timestamps["created_at"]
            if timestamps.get("modified_at"):
                metadata["modified_at"] = timestamps["modified_at"]
            if timestamps.get("viewed_by_me_time"):
                metadata["viewed_by_me_time"] = timestamps["viewed_by_me_time"]

        return metadata

    @staticmethod
    def _get_workspace_type_name(mime_type: str) -> str:
        """Get human-readable name for Google Workspace file type.

        Args:
            mime_type: Google Drive MIME type

        Returns:
            Human-readable type name

        Examples:
            >>> ContentIngestEmitter._get_workspace_type_name("application/vnd.google-apps.document")
            "Google Docs"
            >>> ContentIngestEmitter._get_workspace_type_name("application/vnd.google-apps.spreadsheet")
            "Google Sheets"
        """
        workspace_types = {
            "application/vnd.google-apps.document": "Google Docs",
            "application/vnd.google-apps.spreadsheet": "Google Sheets",
            "application/vnd.google-apps.presentation": "Google Slides",
            "application/vnd.google-apps.form": "Google Forms",
            "application/vnd.google-apps.drawing": "Google Drawings",
            "application/vnd.google-apps.site": "Google Sites",
            "application/vnd.google-apps.script": "Google Apps Script",
            "application/vnd.google-apps.map": "Google My Maps",
            "application/vnd.google-apps.jam": "Google Jamboard",
        }
        return workspace_types.get(mime_type, "Google Workspace file")
