"""Content ingest message emitter for Google Drive worker.

Shared module for building clustera.integration.content.ingest messages from Google
Drive file data. Used by both fetch operations (backfill) and webhook handlers (real-time).

This module encapsulates the logic to:
1. Process Google Drive files (metadata extraction, export format handling)
2. Download file content (export for Workspace files, direct download for others)
3. Upload file content to S3 via FileStorageClient
4. Build content.ingest message envelopes with text and file contents
5. Extract file metadata for structured indexing
6. Handle different file types (Google Workspace files, native files, folders)

All methods maintain the exact same message format for consistency across
backfill and webhook ingestion paths.
"""

from __future__ import annotations

import uuid
from typing import Any, TYPE_CHECKING

import structlog
from clustera_integration_toolkit import build_clusterspace_path
from clustera_integration_toolkit.message import RecordSubmitBuilder

from google_drive_worker.config import GoogleDriveAPIConfig, StorageConfig
from google_drive_worker.normalization.transformer import GoogleDriveDataTransformer
from google_drive_worker.normalization.mime_types import (
    is_folder,
    is_google_workspace_file,
    needs_export,
    get_export_format,
    GOOGLE_FORM,
    GOOGLE_SITE,
    GOOGLE_JAMBOARD,
)
from google_drive_worker.client.s3 import FileStorageClient

if TYPE_CHECKING:
    from google_drive_worker.client.drive_api import GoogleDriveAPIClient


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
    - File content download (export for Workspace files, direct for others)
    - S3 upload for file content storage
    - Text content formatting for different file types
    - Google Workspace file export format handling
    - Folder vs file type detection
    - Message envelope construction with batch context

    Used by:
    - Fetch operations: Backfill file lists (paginated)
    - Webhook handlers: Real-time file change notifications

    Both paths produce identical message formats for consistency.
    """

    # Maximum file size to download (50 MB)
    MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024

    # MIME types that cannot be exported (Forms, Sites, Jamboard)
    NON_EXPORTABLE_WORKSPACE_TYPES = {GOOGLE_FORM, GOOGLE_SITE, GOOGLE_JAMBOARD}

    def __init__(
        self,
        api_config: GoogleDriveAPIConfig,
        storage_config: StorageConfig | None = None,
        logger: structlog.BoundLogger | None = None,
    ) -> None:
        """Initialize content ingest emitter.

        Args:
            api_config: Google Drive API configuration
            storage_config: Storage configuration for S3 uploads
            logger: Structured logger instance (optional, creates new if not provided)
        """
        self.api_config = api_config
        self.storage_config = storage_config or StorageConfig()
        self.logger = logger or structlog.get_logger().bind(
            component="ContentIngestEmitter"
        )
        self.transformer = GoogleDriveDataTransformer()
        self._storage_client: FileStorageClient | None = None

    @property
    def storage_client(self) -> FileStorageClient:
        """Lazy-initialize the storage client."""
        if self._storage_client is None:
            self._storage_client = FileStorageClient(
                config=self.storage_config,
                logger=self.logger,
            )
        return self._storage_client

    async def process_file_for_ingest(
        self,
        api_client: "GoogleDriveAPIClient",
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
        2. File content download (export for Workspace files, direct for others)
        3. S3 upload for file content
        4. Message envelope construction with file content reference

        Args:
            api_client: Google Drive API client for downloading file content
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
            ...     api_client=drive_api_client,
            ...     file_data=drive_file_data,
            ...     connection_config={"integration_connection_id": "conn_abc"},
            ...     customer_id="cust_xyz",
            ...     snowball_id="snow_123",
            ...     batch_context={"batch_id": "batch_001", "batch_sequence": 0, "batch_is_last": False},
            ... )
        """
        # Download file content and upload to S3
        file_content = await self._download_and_upload_content(
            api_client=api_client,
            file_data=file_data,
            customer_id=customer_id,
        )

        # Build content.ingest message envelope
        return self.build_record_submit_message(
            file_data=file_data,
            connection_config=connection_config,
            customer_id=customer_id,
            snowball_id=snowball_id,
            batch_context=batch_context or {},
            file_content=file_content,
        )

    def build_record_submit_message(
        self,
        file_data: dict[str, Any],
        connection_config: dict[str, Any],
        customer_id: str,
        snowball_id: str,
        batch_context: dict[str, Any],
        file_content: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build clustera.integration.content.ingest message for a single file.

        Constructs the complete message envelope including:
        - File metadata as text content (formatted description)
        - File content reference (S3 storage path) if available
        - Structured metadata for indexing
        - Batch context for pagination tracking
        - Idempotency key for deduplication

        Args:
            file_data: Full file data from Google Drive API
            connection_config: Connection configuration
            customer_id: Customer UUID
            snowball_id: Target snowball UUID
            batch_context: Batch metadata (batch_id, batch_sequence, batch_is_last, batch_page_token)
            file_content: Optional file content dict from _download_and_upload_content()
                          with keys: storage_path, mime_type, size_bytes, filename

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
                        {"type": "text", "text": "File: Q4 Report.docx\\nType: Google Docs..."},
                        {"type": "file", "filename": "Q4 Report.docx", "storage_path": "..."}
                    ],
                    "metadata": {
                        "resource_type": "file",
                        "resource_id": "1A2B3C4D5E",
                        "name": "Q4 Report.docx",
                        "mime_type": "application/vnd.google-apps.document",
                        "has_content": true,
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

        # Add file content reference if available
        if file_content is not None:
            contents.append(
                RecordSubmitBuilder.file_content(
                    filename=file_content["filename"],
                    mime_type=file_content["mime_type"],
                    size=file_content["size_bytes"],
                    storage_path=file_content["storage_path"],
                    upload_id=str(uuid.uuid4()),
                )
            )

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

        # Add has_content flag to metadata
        metadata["has_content"] = file_content is not None

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

    async def _download_and_upload_content(
        self,
        api_client: "GoogleDriveAPIClient",
        file_data: dict[str, Any],
        customer_id: str,
    ) -> dict[str, Any] | None:
        """Download file content from Google Drive and upload to S3.

        Handles both Google Workspace files (export) and regular files (direct download).
        Returns None if file content cannot be downloaded (folder, too large, non-exportable).

        Args:
            api_client: Google Drive API client for downloading content
            file_data: Full file data from Google Drive API
            customer_id: Customer UUID for S3 path

        Returns:
            File content dict with keys: storage_path, mime_type, size_bytes, filename
            Or None if file cannot be downloaded (folder, too large, export error)

        Raises:
            No exceptions are raised - errors are logged and None is returned
        """
        file_id: str = file_data.get("id", "")
        file_name: str = file_data.get("name", "unknown")
        mime_type: str = file_data.get("mimeType", "")

        # Skip folders - they have no binary content
        if is_folder(mime_type):
            self.logger.debug(
                "Skipping folder (no content to download)",
                file_id=file_id,
                file_name=file_name,
            )
            return None

        # Skip non-exportable Google Workspace files (Forms, Sites, Jamboard)
        if mime_type in self.NON_EXPORTABLE_WORKSPACE_TYPES:
            self.logger.debug(
                "Skipping non-exportable Google Workspace file",
                file_id=file_id,
                file_name=file_name,
                mime_type=mime_type,
            )
            return None

        # Check file size for non-Workspace files (Workspace files don't have size until exported)
        file_size = file_data.get("size")
        if file_size is not None:
            try:
                size_bytes = int(file_size)
                if size_bytes > self.MAX_FILE_SIZE_BYTES:
                    self.logger.warning(
                        "Skipping large file (exceeds size limit)",
                        file_id=file_id,
                        file_name=file_name,
                        size_bytes=size_bytes,
                        max_size_bytes=self.MAX_FILE_SIZE_BYTES,
                    )
                    return None
            except (ValueError, TypeError):
                pass  # Continue if size can't be parsed

        try:
            # Determine download method based on MIME type
            if needs_export(mime_type):
                # Google Workspace file - export to standard format
                export_format = get_export_format(mime_type)
                if not export_format:
                    self.logger.warning(
                        "No export format available for Google Workspace file",
                        file_id=file_id,
                        file_name=file_name,
                        mime_type=mime_type,
                    )
                    return None

                self.logger.info(
                    "Exporting Google Workspace file",
                    file_id=file_id,
                    file_name=file_name,
                    source_mime_type=mime_type,
                    export_mime_type=export_format,
                )

                content = await api_client.export_file(file_id, export_format)
                content_mime_type = export_format

            else:
                # Regular file - direct download
                self.logger.info(
                    "Downloading file content",
                    file_id=file_id,
                    file_name=file_name,
                    mime_type=mime_type,
                )

                content = await api_client.download_file(file_id)
                content_mime_type = mime_type

            # Check downloaded content size
            if len(content) > self.MAX_FILE_SIZE_BYTES:
                self.logger.warning(
                    "Skipping file (downloaded content exceeds size limit)",
                    file_id=file_id,
                    file_name=file_name,
                    size_bytes=len(content),
                    max_size_bytes=self.MAX_FILE_SIZE_BYTES,
                )
                return None

            # Upload to S3
            upload_result = self.storage_client.upload_file(
                data=content,
                customer_id=customer_id,
                content_type=content_mime_type,
            )

            self.logger.info(
                "File content uploaded to S3",
                file_id=file_id,
                file_name=file_name,
                size_bytes=upload_result.size_bytes,
                storage_path=upload_result.key,
            )

            return {
                "storage_path": upload_result.key,
                "mime_type": content_mime_type,
                "size_bytes": upload_result.size_bytes,
                "filename": file_name,
                "sha256": upload_result.sha256,
            }

        except Exception as e:
            # Log error but don't fail - file metadata will still be ingested
            self.logger.warning(
                "Failed to download/upload file content, continuing with metadata only",
                file_id=file_id,
                file_name=file_name,
                mime_type=mime_type,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

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
                - source_document_url: Direct link to view in Google Drive UI
                - source_document_title: File name (or "(Untitled)" if not set)

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
                "modified_at": "2025-01-20T15:30:00Z",
                "source_document_url": "https://drive.google.com/...",
                "source_document_title": "Q4 Report.docx"
            }
        """
        # Build source document fields for consistent cross-provider metadata
        # source_document_url: Direct link to view in Google Drive UI
        # source_document_title: File name (with fallback to "(Untitled)")
        web_view_link = normalized.get("web_view_link")
        file_name = normalized.get("name")
        source_document_url = web_view_link if web_view_link else None
        source_document_title = file_name if file_name else "(Untitled)"

        metadata: dict[str, Any] = {
            "drive_id": normalized.get("id"),
            "name": file_name,
            "mime_type": normalized.get("mime_type"),
            "category": normalized.get("category"),
            "is_folder": normalized.get("is_folder", False),
            "is_google_workspace": normalized.get("is_google_workspace", False),
            "trashed": normalized.get("trashed", False),
            "starred": normalized.get("starred", False),
            "source_document_url": source_document_url,
            "source_document_title": source_document_title,
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
