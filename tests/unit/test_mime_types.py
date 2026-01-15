"""Unit tests for MIME type utilities."""

import pytest

from google_drive_worker.normalization.mime_types import (
    EXPORT_FORMATS,
    GOOGLE_DOC,
    GOOGLE_DRAWING,
    GOOGLE_FOLDER,
    GOOGLE_FORM,
    GOOGLE_JAMBOARD,
    GOOGLE_SCRIPT,
    GOOGLE_SHEET,
    GOOGLE_SITE,
    GOOGLE_SLIDE,
    GOOGLE_WORKSPACE_TYPES,
    get_export_format,
    get_file_category,
    get_file_extension,
    is_folder,
    is_google_workspace_file,
    needs_export,
)


class TestMimeTypeUtilities:
    """Test suite for MIME type utilities."""

    def test_is_google_workspace_file(self):
        """Test Google Workspace file detection."""
        # Google Workspace types should return True
        assert is_google_workspace_file(GOOGLE_DOC) is True
        assert is_google_workspace_file(GOOGLE_SHEET) is True
        assert is_google_workspace_file(GOOGLE_SLIDE) is True
        assert is_google_workspace_file(GOOGLE_FOLDER) is True
        assert is_google_workspace_file(GOOGLE_FORM) is True
        assert is_google_workspace_file(GOOGLE_DRAWING) is True
        assert is_google_workspace_file(GOOGLE_SITE) is True
        assert is_google_workspace_file(GOOGLE_SCRIPT) is True
        assert is_google_workspace_file(GOOGLE_JAMBOARD) is True

        # Non-Google Workspace types should return False
        assert is_google_workspace_file("application/pdf") is False
        assert is_google_workspace_file("image/jpeg") is False
        assert is_google_workspace_file("text/plain") is False
        assert is_google_workspace_file("video/mp4") is False
        assert is_google_workspace_file("application/vnd.ms-excel") is False

    def test_is_folder(self):
        """Test folder detection."""
        assert is_folder(GOOGLE_FOLDER) is True
        assert is_folder(GOOGLE_DOC) is False
        assert is_folder("application/pdf") is False
        assert is_folder("") is False

    def test_get_export_format(self):
        """Test export format retrieval."""
        # Google Workspace files with export formats (plain text where possible)
        assert get_export_format(GOOGLE_DOC) == "text/plain"
        assert get_export_format(GOOGLE_SHEET) == "text/csv"
        assert (
            get_export_format(GOOGLE_SLIDE)
            == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        )
        assert get_export_format(GOOGLE_DRAWING) == "image/png"
        assert get_export_format(GOOGLE_SCRIPT) == "application/vnd.google-apps.script+json"

        # Files without export formats
        assert get_export_format(GOOGLE_FOLDER) is None
        assert get_export_format(GOOGLE_FORM) is None
        assert get_export_format(GOOGLE_SITE) is None
        assert get_export_format(GOOGLE_JAMBOARD) is None
        assert get_export_format("application/pdf") is None
        assert get_export_format("image/jpeg") is None

    def test_get_file_category(self):
        """Test file categorization."""
        # Google Workspace types
        assert get_file_category(GOOGLE_DOC) == "document"
        assert get_file_category(GOOGLE_SHEET) == "spreadsheet"
        assert get_file_category(GOOGLE_SLIDE) == "presentation"
        assert get_file_category(GOOGLE_FOLDER) == "folder"
        assert get_file_category(GOOGLE_FORM) == "form"
        assert get_file_category(GOOGLE_DRAWING) == "drawing"
        assert get_file_category(GOOGLE_SITE) == "site"
        assert get_file_category(GOOGLE_SCRIPT) == "code"
        assert get_file_category(GOOGLE_JAMBOARD) == "whiteboard"

        # Microsoft Office formats
        assert (
            get_file_category("application/vnd.openxmlformats-officedocument.wordprocessingml.document") == "document"
        )
        assert (
            get_file_category("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") == "spreadsheet"
        )
        assert (
            get_file_category("application/vnd.openxmlformats-officedocument.presentationml.presentation")
            == "presentation"
        )
        assert get_file_category("application/msword") == "document"
        assert get_file_category("application/vnd.ms-excel") == "spreadsheet"
        assert get_file_category("application/vnd.ms-powerpoint") == "presentation"

        # Common document formats
        assert get_file_category("application/pdf") == "document"
        assert get_file_category("text/plain") == "document"
        assert get_file_category("text/html") == "document"
        assert get_file_category("text/csv") == "spreadsheet"
        assert get_file_category("application/rtf") == "document"

        # Image formats
        assert get_file_category("image/jpeg") == "image"
        assert get_file_category("image/png") == "image"
        assert get_file_category("image/gif") == "image"
        assert get_file_category("image/svg+xml") == "image"
        assert get_file_category("image/webp") == "image"
        assert get_file_category("image/unknown") == "image"  # Fallback for image/* types

        # Video formats
        assert get_file_category("video/mp4") == "video"
        assert get_file_category("video/mpeg") == "video"
        assert get_file_category("video/quicktime") == "video"
        assert get_file_category("video/webm") == "video"
        assert get_file_category("video/unknown") == "video"  # Fallback for video/* types

        # Audio formats
        assert get_file_category("audio/mpeg") == "audio"
        assert get_file_category("audio/mp3") == "audio"
        assert get_file_category("audio/wav") == "audio"
        assert get_file_category("audio/ogg") == "audio"
        assert get_file_category("audio/unknown") == "audio"  # Fallback for audio/* types

        # Archive formats
        assert get_file_category("application/zip") == "archive"
        assert get_file_category("application/x-rar-compressed") == "archive"
        assert get_file_category("application/x-tar") == "archive"
        assert get_file_category("application/gzip") == "archive"

        # Code/script formats
        assert get_file_category("text/javascript") == "code"
        assert get_file_category("application/javascript") == "code"
        assert get_file_category("application/json") == "code"
        assert get_file_category("text/x-python") == "code"
        assert get_file_category("text/markdown") == "code"
        assert get_file_category("application/xml") == "code"

        # Text files (fallback)
        assert get_file_category("text/unknown") == "document"

        # Unknown Google Workspace type (fallback)
        assert get_file_category("application/vnd.google-apps.unknown") == "google-workspace"

        # Completely unknown type
        assert get_file_category("application/octet-stream") == "other"
        assert get_file_category("unknown/type") == "other"

    def test_needs_export(self):
        """Test export requirement detection."""
        # Files that need export
        assert needs_export(GOOGLE_DOC) is True
        assert needs_export(GOOGLE_SHEET) is True
        assert needs_export(GOOGLE_SLIDE) is True
        assert needs_export(GOOGLE_DRAWING) is True
        assert needs_export(GOOGLE_SCRIPT) is True

        # Files that don't need export
        assert needs_export(GOOGLE_FOLDER) is False
        assert needs_export(GOOGLE_FORM) is False
        assert needs_export(GOOGLE_SITE) is False
        assert needs_export(GOOGLE_JAMBOARD) is False
        assert needs_export("application/pdf") is False
        assert needs_export("image/jpeg") is False

    def test_get_file_extension(self):
        """Test file extension determination."""
        # Google Workspace (exported to plain text where possible)
        assert get_file_extension(GOOGLE_DOC) == ".txt"
        assert get_file_extension(GOOGLE_SHEET) == ".csv"
        assert get_file_extension(GOOGLE_SLIDE) == ".pptx"
        assert get_file_extension(GOOGLE_DRAWING) == ".png"
        assert get_file_extension(GOOGLE_SCRIPT) == ".json"

        # Documents
        assert get_file_extension("application/pdf") == ".pdf"
        assert get_file_extension("text/plain") == ".txt"
        assert get_file_extension("text/html") == ".html"
        assert get_file_extension("text/csv") == ".csv"
        assert get_file_extension("application/rtf") == ".rtf"
        assert (
            get_file_extension("application/vnd.openxmlformats-officedocument.wordprocessingml.document") == ".docx"
        )
        assert get_file_extension("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") == ".xlsx"
        assert (
            get_file_extension("application/vnd.openxmlformats-officedocument.presentationml.presentation") == ".pptx"
        )
        assert get_file_extension("application/msword") == ".doc"
        assert get_file_extension("application/vnd.ms-excel") == ".xls"
        assert get_file_extension("application/vnd.ms-powerpoint") == ".ppt"

        # Images
        assert get_file_extension("image/jpeg") == ".jpg"
        assert get_file_extension("image/png") == ".png"
        assert get_file_extension("image/gif") == ".gif"
        assert get_file_extension("image/svg+xml") == ".svg"
        assert get_file_extension("image/webp") == ".webp"

        # Videos
        assert get_file_extension("video/mp4") == ".mp4"
        assert get_file_extension("video/mpeg") == ".mpeg"
        assert get_file_extension("video/quicktime") == ".mov"
        assert get_file_extension("video/x-msvideo") == ".avi"
        assert get_file_extension("video/webm") == ".webm"

        # Audio
        assert get_file_extension("audio/mpeg") == ".mp3"
        assert get_file_extension("audio/mp3") == ".mp3"
        assert get_file_extension("audio/wav") == ".wav"
        assert get_file_extension("audio/ogg") == ".ogg"

        # Archives
        assert get_file_extension("application/zip") == ".zip"
        assert get_file_extension("application/x-rar-compressed") == ".rar"
        assert get_file_extension("application/x-tar") == ".tar"
        assert get_file_extension("application/gzip") == ".gz"

        # Code
        assert get_file_extension("text/javascript") == ".js"
        assert get_file_extension("application/javascript") == ".js"
        assert get_file_extension("application/json") == ".json"
        assert get_file_extension("text/x-python") == ".py"
        assert get_file_extension("text/markdown") == ".md"
        assert get_file_extension("text/x-yaml") == ".yaml"
        assert get_file_extension("application/xml") == ".xml"

        # Unknown types
        assert get_file_extension("application/octet-stream") is None
        assert get_file_extension("unknown/type") is None
        assert get_file_extension(GOOGLE_FOLDER) is None  # Folders don't have extensions

    def test_export_formats_consistency(self):
        """Test that export formats are properly defined."""
        # All export formats should be valid MIME types
        for mime_type, export_format in EXPORT_FORMATS.items():
            assert export_format is not None
            assert "/" in export_format  # Basic MIME type check

        # Files with export formats should be marked as needing export
        for mime_type in EXPORT_FORMATS:
            assert needs_export(mime_type) is True

    def test_google_workspace_types_consistency(self):
        """Test that Google Workspace types set is complete."""
        # All defined constants should be in the set
        assert GOOGLE_DOC in GOOGLE_WORKSPACE_TYPES
        assert GOOGLE_SHEET in GOOGLE_WORKSPACE_TYPES
        assert GOOGLE_SLIDE in GOOGLE_WORKSPACE_TYPES
        assert GOOGLE_FOLDER in GOOGLE_WORKSPACE_TYPES
        assert GOOGLE_FORM in GOOGLE_WORKSPACE_TYPES
        assert GOOGLE_DRAWING in GOOGLE_WORKSPACE_TYPES
        assert GOOGLE_SITE in GOOGLE_WORKSPACE_TYPES
        assert GOOGLE_SCRIPT in GOOGLE_WORKSPACE_TYPES
        assert GOOGLE_JAMBOARD in GOOGLE_WORKSPACE_TYPES

        # All items in set should be Google Workspace types
        for mime_type in GOOGLE_WORKSPACE_TYPES:
            assert mime_type.startswith("application/vnd.google-apps.")