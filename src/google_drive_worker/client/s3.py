"""File storage client using the shared toolkit.

Provides a thin wrapper around the toolkit's ObjectStorageClient for
Google Drive file content handling. Files larger than 256KB are
uploaded to S3-compatible storage instead of being included inline in
Kafka messages.

Uses content-addressable storage - identical content is deduplicated
automatically based on SHA256 hash.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from google_drive_worker.config import StorageConfig

if TYPE_CHECKING:
    from clustera_integration_toolkit.storage import ObjectStorageClient, UploadResult


@dataclass(frozen=True)
class FileUploadResult:
    """Result of a file upload operation.

    Attributes:
        url: Full URL to access the file.
        sha256: SHA256 hash of the file content.
        size_bytes: Size of the file in bytes.
        key: Object key in the bucket.
    """

    url: str
    sha256: str
    size_bytes: int
    key: str


class FileStorageClient:
    """Client for uploading Google Drive file content to S3-compatible storage.

    Uses the shared toolkit's ObjectStorageClient with content-addressable
    paths for automatic deduplication. Same file content across different
    Drive files will only be stored once.

    Path structure:
        {customer_hex[0:2]}/{customer_hex[2:4]}/{customer_uuid}/{hash[0:2]}/{hash[2:4]}/{hash}

    Example:
        storage = FileStorageClient(config)
        result = storage.upload_file(
            data=file_bytes,
            customer_id="550e8400-e29b-41d4-a716-446655440000",
            content_type="application/pdf",
        )
        print(result.url)  # URL to fetch the file
    """

    # Threshold for storage offloading (256KB)
    OFFLOAD_THRESHOLD = 256 * 1024

    def __init__(
        self,
        config: StorageConfig,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        """Initialize file storage client.

        Args:
            config: Storage configuration (controls mock mode).
            logger: Optional structured logger.
        """
        self.config = config
        self.logger = (logger or structlog.get_logger()).bind(
            client="FileStorageClient"
        )
        self._storage_client: ObjectStorageClient | None = None

    @property
    def storage_client(self) -> "ObjectStorageClient":
        """Lazy-initialize the toolkit's storage client."""
        if self._storage_client is None:
            from clustera_integration_toolkit.storage import (
                ObjectStorageClient,
                ObjectStorageConfig,
            )

            storage_config = ObjectStorageConfig.from_env()
            self._storage_client = ObjectStorageClient(storage_config)

        return self._storage_client

    def should_offload(self, data: bytes) -> bool:
        """Check if data should be offloaded to storage.

        Args:
            data: Raw file data.

        Returns:
            True if size exceeds the 256KB threshold.
        """
        return len(data) > self.OFFLOAD_THRESHOLD

    @staticmethod
    def compute_sha256(data: bytes) -> str:
        """Compute SHA-256 hash of data.

        Args:
            data: Raw data.

        Returns:
            Lowercase hex-encoded SHA-256 hash (64 characters).
        """
        return hashlib.sha256(data).hexdigest()

    def upload_file(
        self,
        data: bytes,
        customer_id: str,
        content_type: str = "application/octet-stream",
    ) -> FileUploadResult:
        """Upload file to storage.

        Uses content-addressable storage - the same content uploaded twice
        will result in the same URL (automatic deduplication).

        Args:
            data: Raw file data.
            customer_id: Customer UUID for path hierarchy.
            content_type: MIME type of the file.

        Returns:
            FileUploadResult with URL, hash, size, and key.

        Raises:
            botocore.exceptions.ClientError: If upload fails.
        """
        if self.config.mock_mode:
            sha256 = self.compute_sha256(data)
            mock_key = f"mock/{customer_id[:8]}/{sha256[:16]}"
            mock_url = f"s3://mock-bucket/{mock_key}"

            self.logger.info(
                "[MOCK] Would upload file to storage",
                customer_id=customer_id[:8] + "...",
                size_bytes=len(data),
                sha256=sha256[:16] + "...",
            )

            return FileUploadResult(
                url=mock_url,
                sha256=sha256,
                size_bytes=len(data),
                key=mock_key,
            )

        self.logger.debug(
            "Uploading file to storage",
            customer_id=customer_id[:8] + "...",
            size_bytes=len(data),
            content_type=content_type,
        )

        result = self.storage_client.upload(
            content=data,
            customer_id=customer_id,
            content_type=content_type,
        )

        self.logger.info(
            "File uploaded successfully",
            customer_id=customer_id[:8] + "...",
            size_bytes=result.size_bytes,
            sha256=result.sha256[:16] + "...",
        )

        return FileUploadResult(
            url=result.url,
            sha256=result.sha256,
            size_bytes=result.size_bytes,
            key=result.key,
        )

    def get_file_url(self, customer_id: str, content_hash: str) -> str:
        """Get URL for a file without uploading.

        Useful for checking if content already exists or generating
        a reference URL.

        Args:
            customer_id: Customer UUID.
            content_hash: SHA256 hash of the content.

        Returns:
            Full URL to access the file.
        """
        if self.config.mock_mode:
            return f"s3://mock-bucket/mock/{customer_id[:8]}/{content_hash[:16]}"

        return self.storage_client.get_url(customer_id, content_hash)

    def close(self) -> None:
        """Close the storage client connection."""
        if self._storage_client is not None:
            self._storage_client.close()
            self._storage_client = None

    def __enter__(self) -> "FileStorageClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[no-untyped-def]
        """Context manager exit - close client."""
        self.close()
