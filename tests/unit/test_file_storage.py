"""Unit tests for FileStorageClient (S3 file content storage)."""

import pytest
import hashlib
from unittest.mock import Mock, MagicMock, patch

from google_drive_worker.client.s3 import FileStorageClient, FileUploadResult
from google_drive_worker.config import StorageConfig


class TestFileStorageClient:
    """Test cases for FileStorageClient."""

    @pytest.fixture
    def mock_storage_config(self):
        """Create a mock storage config in mock mode."""
        config = StorageConfig()
        config.mock_mode = True
        return config

    @pytest.fixture
    def real_storage_config(self):
        """Create a storage config in real mode."""
        config = StorageConfig()
        config.mock_mode = False
        return config

    @pytest.fixture
    def storage_client_mock(self, mock_storage_config):
        """Create a storage client in mock mode."""
        return FileStorageClient(config=mock_storage_config)

    @pytest.fixture
    def sample_file_content(self):
        """Create sample file content."""
        return b"Sample file content for testing purposes."

    @pytest.fixture
    def large_file_content(self):
        """Create content larger than offload threshold (256KB)."""
        return b"x" * (300 * 1024)  # 300KB

    @pytest.fixture
    def small_file_content(self):
        """Create content smaller than offload threshold."""
        return b"Small file content"

    # Mock mode tests

    def test_upload_file_mock_mode(self, storage_client_mock, sample_file_content):
        """Test file upload in mock mode."""
        customer_id = "550e8400-e29b-41d4-a716-446655440000"

        result = storage_client_mock.upload_file(
            data=sample_file_content,
            customer_id=customer_id,
            content_type="application/pdf",
        )

        # Verify result type and structure
        assert isinstance(result, FileUploadResult)
        assert result.url.startswith("s3://mock-bucket/")
        assert result.size_bytes == len(sample_file_content)
        assert len(result.sha256) == 64  # SHA-256 hex digest

        # Verify SHA256 calculation
        expected_sha256 = hashlib.sha256(sample_file_content).hexdigest()
        assert result.sha256 == expected_sha256

    def test_upload_file_mock_mode_key_format(self, storage_client_mock, sample_file_content):
        """Test that mock mode generates correct key format."""
        customer_id = "550e8400-e29b-41d4-a716-446655440000"

        result = storage_client_mock.upload_file(
            data=sample_file_content,
            customer_id=customer_id,
            content_type="application/pdf",
        )

        # Key should contain partial customer_id and partial hash
        assert customer_id[:8] in result.key
        assert "mock/" in result.key

    def test_get_file_url_mock_mode(self, storage_client_mock):
        """Test getting file URL in mock mode."""
        customer_id = "550e8400-e29b-41d4-a716-446655440000"
        content_hash = "a" * 64

        url = storage_client_mock.get_file_url(customer_id, content_hash)

        assert url.startswith("s3://mock-bucket/")
        assert customer_id[:8] in url
        assert content_hash[:16] in url

    # Hash calculation tests

    def test_compute_sha256_correct(self, storage_client_mock, sample_file_content):
        """Test SHA-256 hash computation."""
        result = FileStorageClient.compute_sha256(sample_file_content)

        expected = hashlib.sha256(sample_file_content).hexdigest()
        assert result == expected
        assert len(result) == 64

    def test_compute_sha256_empty_data(self, storage_client_mock):
        """Test SHA-256 hash of empty data."""
        result = FileStorageClient.compute_sha256(b"")

        # Empty SHA-256 hash
        expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        assert result == expected

    def test_compute_sha256_deterministic(self, storage_client_mock, sample_file_content):
        """Test SHA-256 is deterministic (same input = same output)."""
        result1 = FileStorageClient.compute_sha256(sample_file_content)
        result2 = FileStorageClient.compute_sha256(sample_file_content)

        assert result1 == result2

    # Offload threshold tests

    def test_should_offload_large_file(self, storage_client_mock, large_file_content):
        """Test that large files should be offloaded."""
        assert storage_client_mock.should_offload(large_file_content) is True

    def test_should_offload_small_file(self, storage_client_mock, small_file_content):
        """Test that small files should not be offloaded."""
        assert storage_client_mock.should_offload(small_file_content) is False

    def test_should_offload_exact_threshold(self, storage_client_mock):
        """Test behavior at exactly 256KB threshold."""
        # Exactly at threshold should not be offloaded (> not >=)
        threshold_data = b"x" * (256 * 1024)
        assert storage_client_mock.should_offload(threshold_data) is False

        # One byte over should be offloaded
        over_threshold_data = b"x" * (256 * 1024 + 1)
        assert storage_client_mock.should_offload(over_threshold_data) is True

    # Context manager tests

    def test_context_manager_enter_exit(self, mock_storage_config):
        """Test context manager protocol."""
        with FileStorageClient(config=mock_storage_config) as client:
            assert client is not None
            assert isinstance(client, FileStorageClient)

    def test_close_clears_storage_client(self, mock_storage_config):
        """Test that close() clears the internal storage client."""
        client = FileStorageClient(config=mock_storage_config)
        client._storage_client = Mock()  # Simulate initialized client

        client.close()

        assert client._storage_client is None

    # Real mode tests (mocking toolkit)

    def test_upload_file_real_mode(self, real_storage_config, sample_file_content):
        """Test file upload in real mode with mocked toolkit."""
        client = FileStorageClient(config=real_storage_config)

        # Create a mock for the storage client
        mock_storage = Mock()
        mock_storage.upload.return_value = Mock(
            url="s3://real-bucket/path/to/file",
            sha256="abc123def456",
            size_bytes=len(sample_file_content),
            key="path/to/file",
        )
        client._storage_client = mock_storage

        result = client.upload_file(
            data=sample_file_content,
            customer_id="550e8400-e29b-41d4-a716-446655440000",
            content_type="application/pdf",
        )

        # Verify upload was called
        mock_storage.upload.assert_called_once_with(
            content=sample_file_content,
            customer_id="550e8400-e29b-41d4-a716-446655440000",
            content_type="application/pdf",
        )

        # Verify result
        assert isinstance(result, FileUploadResult)
        assert result.url == "s3://real-bucket/path/to/file"

    # Edge cases

    def test_upload_file_different_content_types(self, storage_client_mock, sample_file_content):
        """Test upload with different content types."""
        customer_id = "550e8400-e29b-41d4-a716-446655440000"

        content_types = [
            "application/pdf",
            "image/png",
            "image/jpeg",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "text/plain",
            "application/octet-stream",
        ]

        for content_type in content_types:
            result = storage_client_mock.upload_file(
                data=sample_file_content,
                customer_id=customer_id,
                content_type=content_type,
            )
            assert isinstance(result, FileUploadResult)

    def test_upload_file_binary_content(self, storage_client_mock):
        """Test upload with actual binary content (not just ASCII)."""
        customer_id = "550e8400-e29b-41d4-a716-446655440000"

        # Simulate PNG header
        binary_content = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00"

        result = storage_client_mock.upload_file(
            data=binary_content,
            customer_id=customer_id,
            content_type="image/png",
        )

        assert result.size_bytes == len(binary_content)
        assert result.sha256 == hashlib.sha256(binary_content).hexdigest()

    def test_offload_threshold_constant(self, storage_client_mock):
        """Test that the offload threshold constant is 256KB."""
        assert FileStorageClient.OFFLOAD_THRESHOLD == 256 * 1024


class TestFileUploadResult:
    """Test cases for FileUploadResult dataclass."""

    def test_frozen_dataclass(self):
        """Test that FileUploadResult is immutable."""
        result = FileUploadResult(
            url="s3://bucket/key",
            sha256="abc123",
            size_bytes=1000,
            key="key",
        )

        with pytest.raises(AttributeError):
            result.url = "new_url"

    def test_all_fields_required(self):
        """Test that all fields are required."""
        with pytest.raises(TypeError):
            FileUploadResult(url="s3://bucket/key")

    def test_field_access(self):
        """Test field access on FileUploadResult."""
        result = FileUploadResult(
            url="s3://bucket/test/key",
            sha256="abc123def456",
            size_bytes=2048,
            key="test/key",
        )

        assert result.url == "s3://bucket/test/key"
        assert result.sha256 == "abc123def456"
        assert result.size_bytes == 2048
        assert result.key == "test/key"
