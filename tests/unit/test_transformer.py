"""Unit tests for GoogleDriveDataTransformer."""

import json
from pathlib import Path

import pytest

from google_drive_worker.normalization.transformer import GoogleDriveDataTransformer


class TestGoogleDriveDataTransformer:
    """Test suite for GoogleDriveDataTransformer."""

    @pytest.fixture
    def transformer(self):
        """Create transformer instance."""
        return GoogleDriveDataTransformer()

    @pytest.fixture
    def mock_responses(self):
        """Load mock API responses."""
        fixtures_path = Path(__file__).parent.parent / "fixtures" / "mock_drive_responses.json"
        with open(fixtures_path) as f:
            return json.load(f)

    @pytest.fixture
    def sample_google_doc(self, mock_responses):
        """Sample Google Doc API response."""
        return mock_responses["google_doc"]

    @pytest.fixture
    def sample_google_sheet(self, mock_responses):
        """Sample Google Sheet API response."""
        return mock_responses["google_sheet"]

    @pytest.fixture
    def sample_google_slide(self, mock_responses):
        """Sample Google Slide API response."""
        return mock_responses["google_slide"]

    @pytest.fixture
    def sample_folder(self, mock_responses):
        """Sample folder API response."""
        return mock_responses["google_folder"]

    @pytest.fixture
    def sample_pdf(self, mock_responses):
        """Sample PDF file API response."""
        return mock_responses["pdf_file"]

    @pytest.fixture
    def sample_image(self, mock_responses):
        """Sample image file API response."""
        return mock_responses["image_file"]

    def test_transform_google_doc(self, transformer, sample_google_doc):
        """Test transformation of Google Doc."""
        result = transformer.transform_file(
            sample_google_doc, connection_id="conn_123", customer_id="cust_456"
        )

        # Basic fields
        assert result["id"] == "1A2B3C4D5E6F7G8H9I0"
        assert result["type"] == "file"
        assert result["name"] == "Q4 Financial Report"
        assert result["mime_type"] == "application/vnd.google-apps.document"
        assert result["category"] == "document"

        # Google Workspace specific
        assert result["is_google_workspace"] is True
        assert result["is_folder"] is False
        assert result["needs_export"] is True
        assert result["export_format"] == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        assert result["file_extension"] == ".docx"

        # Size (should be None for Google Workspace files)
        assert result["size_bytes"] is None

        # Links
        assert result["web_view_link"] == "https://docs.google.com/document/d/1A2B3C4D5E6F7G8H9I0/edit"
        assert result["thumbnail_link"] == "https://lh3.googleusercontent.com/thumbnail1"

        # Parent folders
        assert result["parent_ids"] == ["0B1C2D3E4F5G6H7I8J9"]

        # Owners
        assert len(result["owners"]) == 1
        assert result["owners"][0]["name"] == "Alice Johnson"
        assert result["owners"][0]["email"] == "alice@example.com"
        assert result["owners"][0]["photo_link"] == "https://lh3.googleusercontent.com/a/alice"

        # Last modifying user
        assert result["last_modified_by"]["name"] == "Bob Smith"
        assert result["last_modified_by"]["email"] == "bob@example.com"

        # Capabilities
        assert result["capabilities"]["can_edit"] is True
        assert result["capabilities"]["can_comment"] is True
        assert result["capabilities"]["can_share"] is True
        assert result["capabilities"]["can_delete"] is False
        assert result["capabilities"]["can_add_children"] is False

        # Flags
        assert result["trashed"] is False
        assert result["starred"] is True
        assert result["shared"] is True
        assert result["viewed_by_me"] is True

        # Timestamps
        assert result["timestamps"]["created_at"] == "2025-01-15T10:00:00.000Z"
        assert result["timestamps"]["modified_at"] == "2025-01-20T15:30:00.000Z"
        assert result["timestamps"]["viewed_by_me_at"] == "2025-01-21T09:15:00.000Z"
        assert result["timestamps"]["shared_with_me_at"] == "2025-01-18T14:20:00.000Z"

        # Version and description
        assert result["version"] == "5"
        assert result["description"] == "Quarterly financial report for Q4 2024"

        # Source metadata
        assert result["source"]["provider"] == "google-drive"
        assert result["source"]["file_id"] == "1A2B3C4D5E6F7G8H9I0"
        assert result["source"]["connection_id"] == "conn_123"
        assert result["source"]["customer_id"] == "cust_456"
        assert result["source"]["transformation_version"] == "1.0.0"
        assert "transformed_at" in result["source"]

        # Metadata
        assert result["_metadata"]["original_field_count"] == len(sample_google_doc)

    def test_transform_google_sheet(self, transformer, sample_google_sheet):
        """Test transformation of Google Sheet."""
        result = transformer.transform_file(
            sample_google_sheet, connection_id="conn_234", customer_id="cust_567"
        )

        assert result["id"] == "2B3C4D5E6F7G8H9I0J1"
        assert result["name"] == "Budget Tracker 2025"
        assert result["mime_type"] == "application/vnd.google-apps.spreadsheet"
        assert result["category"] == "spreadsheet"
        assert result["is_google_workspace"] is True
        assert result["export_format"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        assert result["file_extension"] == ".xlsx"
        assert result["web_content_link"] == "https://docs.google.com/spreadsheets/export?id=2B3C4D5E6F7G8H9I0J1"

    def test_transform_google_slide(self, transformer, sample_google_slide):
        """Test transformation of Google Slide."""
        result = transformer.transform_file(
            sample_google_slide, connection_id="conn_345", customer_id="cust_678"
        )

        assert result["id"] == "3C4D5E6F7G8H9I0J1K2"
        assert result["name"] == "Company Presentation"
        assert result["mime_type"] == "application/vnd.google-apps.presentation"
        assert result["category"] == "presentation"
        assert result["is_google_workspace"] is True
        assert result["export_format"] == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        assert result["file_extension"] == ".pptx"
        assert result["quota_bytes_used"] == 0  # String "0" converted to int

    def test_transform_folder(self, transformer, sample_folder):
        """Test transformation of folder."""
        result = transformer.transform_file(
            sample_folder, connection_id="conn_456", customer_id="cust_789"
        )

        assert result["id"] == "4D5E6F7G8H9I0J1K2L3"
        assert result["type"] == "folder"
        assert result["name"] == "Project Documents"
        assert result["mime_type"] == "application/vnd.google-apps.folder"
        assert result["category"] == "folder"
        assert result["is_folder"] is True
        assert result["is_google_workspace"] is True
        assert result["needs_export"] is False
        assert "export_format" not in result  # Folders don't have export format
        assert result["parent_ids"] == ["root"]
        assert result["folder_color_rgb"] == "#1e90ff"
        assert result["capabilities"]["can_add_children"] is True

    def test_transform_pdf(self, transformer, sample_pdf):
        """Test transformation of regular PDF file."""
        result = transformer.transform_file(sample_pdf, connection_id="conn_567", customer_id="cust_890")

        assert result["id"] == "5E6F7G8H9I0J1K2L3M4"
        assert result["type"] == "file"
        assert result["name"] == "Contract_Agreement.pdf"
        assert result["mime_type"] == "application/pdf"
        assert result["category"] == "document"
        assert result["is_folder"] is False
        assert result["is_google_workspace"] is False
        assert result["needs_export"] is False
        assert "export_format" not in result
        assert result["file_extension"] == ".pdf"
        assert result["size_bytes"] == 524288
        assert result["md5_checksum"] == "d41d8cd98f00b204e9800998ecf8427e"
        assert result["sha1_checksum"] == "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        assert result["sha256_checksum"] == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_transform_image(self, transformer, sample_image):
        """Test transformation of image file."""
        result = transformer.transform_file(sample_image, connection_id="conn_678", customer_id="cust_901")

        assert result["id"] == "6F7G8H9I0J1K2L3M4N5"
        assert result["name"] == "team_photo.jpg"
        assert result["mime_type"] == "image/jpeg"
        assert result["category"] == "image"
        assert result["is_google_workspace"] is False
        assert result["file_extension"] == ".jpg"
        assert result["size_bytes"] == 2097152
        assert result["starred"] is True

    def test_transform_file_without_parents(self, transformer, mock_responses):
        """Test handling of files at root level."""
        file_data = mock_responses["file_without_parents"]
        result = transformer.transform_file(file_data, connection_id="conn_789", customer_id="cust_012")

        assert result["id"] == "7G8H9I0J1K2L3M4N5O6"
        assert result["parent_ids"] == []  # No parents field means empty list
        assert result["size_bytes"] == 1024

    def test_transform_file_missing_optional_fields(self, transformer, mock_responses):
        """Test handling of minimal API response."""
        minimal_file = mock_responses["minimal_file"]
        result = transformer.transform_file(minimal_file, connection_id="conn_890", customer_id="cust_123")

        # Required fields should be present
        assert result["id"] == "8H9I0J1K2L3M4N5O6P7"
        assert result["name"] == "minimal.txt"
        assert result["mime_type"] == "text/plain"
        assert result["type"] == "file"
        assert result["category"] == "document"

        # Optional fields should have defaults
        assert result["parent_ids"] == []
        assert result["owners"] == []
        assert result["trashed"] is False
        assert result["starred"] is False
        assert result["shared"] is False
        assert result["viewed_by_me"] is False
        assert result["timestamps"] == {}

        # Source metadata should still be present
        assert result["source"]["provider"] == "google-drive"
        assert result["source"]["file_id"] == "8H9I0J1K2L3M4N5O6P7"

    def test_transform_trashed_file(self, transformer, mock_responses):
        """Test transformation of trashed file."""
        trashed_file = mock_responses["trashed_file"]
        result = transformer.transform_file(trashed_file, connection_id="conn_901", customer_id="cust_234")

        assert result["id"] == "9I0J1K2L3M4N5O6P7Q8"
        assert result["trashed"] is True
        assert result["explicitly_trashed"] is True
        assert result["timestamps"]["trashed_at"] == "2025-01-10T10:00:00.000Z"
        assert result["quota_bytes_used"] == 0  # Trashed files don't use quota

    def test_transform_video_file(self, transformer, mock_responses):
        """Test transformation of video file."""
        video_file = mock_responses["video_file"]
        result = transformer.transform_file(video_file, connection_id="conn_012", customer_id="cust_345")

        assert result["id"] == "0J1K2L3M4N5O6P7Q8R9"
        assert result["mime_type"] == "video/mp4"
        assert result["category"] == "video"
        assert result["file_extension"] == ".mp4"
        assert result["size_bytes"] == 52428800

    def test_transform_google_form(self, transformer, mock_responses):
        """Test transformation of Google Form."""
        form = mock_responses["google_form"]
        result = transformer.transform_file(form, connection_id="conn_123", customer_id="cust_456")

        assert result["id"] == "1K2L3M4N5O6P7Q8R9S0"
        assert result["mime_type"] == "application/vnd.google-apps.form"
        assert result["category"] == "form"
        assert result["is_google_workspace"] is True
        assert result["capabilities"]["can_download"] is False  # Forms can't be downloaded

    def test_transform_revision(self, transformer, mock_responses):
        """Test transformation of file revision."""
        revision = mock_responses["revision_sample"]
        result = transformer.transform_revision(
            revision, file_id="1A2B3C", connection_id="conn_234", customer_id="cust_567"
        )

        assert result["id"] == "1000"
        assert result["file_id"] == "1A2B3C"
        assert result["type"] == "revision"
        assert result["keep_forever"] is True
        assert result["published"] is False
        assert result["mime_type"] == "application/vnd.google-apps.document"
        assert result["size_bytes"] == 12345
        assert result["md5_checksum"] == "abc123def456"
        assert result["last_modified_by"]["name"] == "Editor User"
        assert result["last_modified_by"]["email"] == "editor@example.com"
        assert result["timestamps"]["modified_at"] == "2025-01-19T10:00:00.000Z"
        assert "export_links" in result
        assert result["source"]["provider"] == "google-drive"
        assert result["source"]["revision_id"] == "1000"

    def test_transform_permission_anyone(self, transformer, mock_responses):
        """Test transformation of 'anyone' permission."""
        permission = mock_responses["permission_sample"]
        result = transformer.transform_permission(
            permission, file_id="1A2B3C", connection_id="conn_345", customer_id="cust_678"
        )

        assert result["id"] == "anyoneWithLink"
        assert result["file_id"] == "1A2B3C"
        assert result["type"] == "permission"
        assert result["permission_type"] == "anyone"
        assert result["role"] == "reader"
        assert result["display_name"] == "Anyone with the link"
        assert result["allow_file_discovery"] is False

    def test_transform_permission_user(self, transformer, mock_responses):
        """Test transformation of user permission."""
        permission = mock_responses["permission_user"]
        result = transformer.transform_permission(
            permission, file_id="2B3C4D", connection_id="conn_456", customer_id="cust_789"
        )

        assert result["id"] == "12345678901234567890"
        assert result["permission_type"] == "user"
        assert result["role"] == "writer"
        assert result["email_address"] == "collaborator@example.com"
        assert result["display_name"] == "Collaborator Name"
        assert result["expiration_time"] == "2025-02-28T23:59:59.000Z"

    def test_transform_permission_domain(self, transformer, mock_responses):
        """Test transformation of domain permission."""
        permission = mock_responses["permission_domain"]
        result = transformer.transform_permission(
            permission, file_id="3C4D5E", connection_id="conn_567", customer_id="cust_890"
        )

        assert result["id"] == "domain123"
        assert result["permission_type"] == "domain"
        assert result["role"] == "commenter"
        assert result["domain"] == "example.com"
        assert result["allow_file_discovery"] is True

    def test_transform_comment(self, transformer, mock_responses):
        """Test transformation of comment with replies."""
        comment = mock_responses["comment_sample"]
        result = transformer.transform_comment(
            comment, file_id="1A2B3C", connection_id="conn_678", customer_id="cust_901"
        )

        assert result["id"] == "comment123"
        assert result["file_id"] == "1A2B3C"
        assert result["type"] == "comment"
        assert result["content"] == "This section needs review"
        assert result["resolved"] is False
        assert result["author"]["name"] == "Reviewer"
        assert result["author"]["email"] == "reviewer@example.com"
        assert result["timestamps"]["created_at"] == "2025-01-20T14:00:00.000Z"

        # Check replies
        assert len(result["replies"]) == 1
        assert result["reply_count"] == 1
        reply = result["replies"][0]
        assert reply["id"] == "reply456"
        assert reply["content"] == "I'll update this section"
        assert reply["author"]["name"] == "Author"

    def test_transform_files_batch(self, transformer, mock_responses):
        """Test batch transformation of multiple files."""
        files = [
            mock_responses["google_doc"],
            mock_responses["pdf_file"],
            mock_responses["google_folder"],
        ]

        results = transformer.transform_files_batch(files, connection_id="conn_batch", customer_id="cust_batch")

        assert len(results) == 3
        assert results[0]["name"] == "Q4 Financial Report"
        assert results[1]["name"] == "Contract_Agreement.pdf"
        assert results[2]["name"] == "Project Documents"

    def test_transform_files_batch_with_error(self, transformer, mock_responses, caplog):
        """Test batch transformation handles invalid data gracefully."""
        files = [
            mock_responses["google_doc"],
            {"invalid": "data"},  # This will be processed with defaults
            mock_responses["pdf_file"],
        ]

        results = transformer.transform_files_batch(files, connection_id="conn_error", customer_id="cust_error")

        # All files are processed (invalid data gets defaults)
        assert len(results) == 3
        assert results[0]["name"] == "Q4 Financial Report"
        assert results[1]["name"] == "Unnamed"  # Default name for invalid data
        assert results[1]["id"] == ""  # Empty ID for invalid data
        assert results[2]["name"] == "Contract_Agreement.pdf"

        # Invalid file should have minimal fields with defaults
        invalid_result = results[1]
        assert invalid_result["type"] == "file"
        assert invalid_result["mime_type"] == "application/octet-stream"
        assert invalid_result["category"] == "other"

    def test_parse_timestamp_with_utc(self, transformer):
        """Test timestamp parsing with UTC indicator."""
        # Already has Z suffix
        ts1 = transformer._parse_timestamp("2025-01-20T10:00:00.000Z")
        assert ts1 == "2025-01-20T10:00:00.000Z"

        # Has +00:00 suffix (should be converted to Z)
        ts2 = transformer._parse_timestamp("2025-01-20T10:00:00.000+00:00")
        assert ts2 == "2025-01-20T10:00:00.000Z"

        # No suffix (should be left as is)
        ts3 = transformer._parse_timestamp("2025-01-20T10:00:00.000")
        assert ts3 == "2025-01-20T10:00:00.000"

    def test_size_conversion_edge_cases(self, transformer):
        """Test size field conversion edge cases."""
        # Valid string size
        file1 = {"id": "1", "name": "file1.txt", "mimeType": "text/plain", "size": "12345"}
        result1 = transformer.transform_file(file1, "conn", "cust")
        assert result1["size_bytes"] == 12345

        # Invalid string size
        file2 = {"id": "2", "name": "file2.txt", "mimeType": "text/plain", "size": "not_a_number"}
        result2 = transformer.transform_file(file2, "conn", "cust")
        assert result2["size_bytes"] == 0

        # None size
        file3 = {"id": "3", "name": "file3.txt", "mimeType": "text/plain", "size": None}
        result3 = transformer.transform_file(file3, "conn", "cust")
        assert result3["size_bytes"] == 0

        # Missing size (Google Workspace file)
        file4 = {"id": "4", "name": "doc.gdoc", "mimeType": "application/vnd.google-apps.document"}
        result4 = transformer.transform_file(file4, "conn", "cust")
        assert result4["size_bytes"] is None