"""Google Drive MIME type utilities and constants."""

from typing import Optional

# Google Workspace MIME type constants
GOOGLE_DOC = "application/vnd.google-apps.document"
GOOGLE_SHEET = "application/vnd.google-apps.spreadsheet"
GOOGLE_SLIDE = "application/vnd.google-apps.presentation"
GOOGLE_FOLDER = "application/vnd.google-apps.folder"
GOOGLE_FORM = "application/vnd.google-apps.form"
GOOGLE_DRAWING = "application/vnd.google-apps.drawing"
GOOGLE_SITE = "application/vnd.google-apps.site"
GOOGLE_SCRIPT = "application/vnd.google-apps.script"
GOOGLE_JAMBOARD = "application/vnd.google-apps.jam"

# Export format mappings for Google Workspace files
# Prefer plain text formats where possible for better downstream processing
EXPORT_FORMATS = {
    GOOGLE_DOC: "text/plain",  # .txt - plain text for better LLM processing
    GOOGLE_SHEET: "text/csv",  # .csv - plain text tabular data
    GOOGLE_SLIDE: "application/vnd.openxmlformats-officedocument.presentationml.presentation",  # .pptx - no plain text option
    GOOGLE_DRAWING: "image/png",  # .png - no plain text option for images
    GOOGLE_SCRIPT: "application/vnd.google-apps.script+json",  # .json - already text-based
}

# Set of all Google Workspace MIME types
GOOGLE_WORKSPACE_TYPES = {
    GOOGLE_DOC,
    GOOGLE_SHEET,
    GOOGLE_SLIDE,
    GOOGLE_FOLDER,
    GOOGLE_FORM,
    GOOGLE_DRAWING,
    GOOGLE_SITE,
    GOOGLE_SCRIPT,
    GOOGLE_JAMBOARD,
}

# File category mappings
CATEGORY_MAPPINGS = {
    # Google Workspace types
    GOOGLE_DOC: "document",
    GOOGLE_SHEET: "spreadsheet",
    GOOGLE_SLIDE: "presentation",
    GOOGLE_FOLDER: "folder",
    GOOGLE_FORM: "form",
    GOOGLE_DRAWING: "drawing",
    GOOGLE_SITE: "site",
    GOOGLE_SCRIPT: "code",
    GOOGLE_JAMBOARD: "whiteboard",

    # Microsoft Office formats
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "spreadsheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "presentation",
    "application/msword": "document",
    "application/vnd.ms-excel": "spreadsheet",
    "application/vnd.ms-powerpoint": "presentation",

    # Common document formats
    "application/pdf": "document",
    "text/plain": "document",
    "text/html": "document",
    "text/csv": "spreadsheet",
    "application/rtf": "document",

    # Image formats
    "image/jpeg": "image",
    "image/png": "image",
    "image/gif": "image",
    "image/svg+xml": "image",
    "image/webp": "image",
    "image/bmp": "image",
    "image/tiff": "image",

    # Video formats
    "video/mp4": "video",
    "video/mpeg": "video",
    "video/quicktime": "video",
    "video/x-msvideo": "video",
    "video/webm": "video",

    # Audio formats
    "audio/mpeg": "audio",
    "audio/mp3": "audio",
    "audio/wav": "audio",
    "audio/ogg": "audio",
    "audio/webm": "audio",

    # Archive formats
    "application/zip": "archive",
    "application/x-rar-compressed": "archive",
    "application/x-tar": "archive",
    "application/gzip": "archive",
    "application/x-7z-compressed": "archive",

    # Code/script formats
    "text/javascript": "code",
    "application/javascript": "code",
    "application/json": "code",
    "text/x-python": "code",
    "application/x-python-code": "code",
    "text/x-java-source": "code",
    "text/x-c": "code",
    "text/x-c++": "code",
    "text/markdown": "code",
    "text/x-yaml": "code",
    "application/xml": "code",
    "text/xml": "code",
}


def is_google_workspace_file(mime_type: str) -> bool:
    """
    Check if file is a Google Workspace file.

    Args:
        mime_type: MIME type string

    Returns:
        True if the file is a Google Workspace file, False otherwise
    """
    return mime_type in GOOGLE_WORKSPACE_TYPES


def is_folder(mime_type: str) -> bool:
    """
    Check if item is a folder.

    Args:
        mime_type: MIME type string

    Returns:
        True if the item is a folder, False otherwise
    """
    return mime_type == GOOGLE_FOLDER


def get_export_format(mime_type: str) -> Optional[str]:
    """
    Get export format for Google Workspace files.

    Args:
        mime_type: MIME type string

    Returns:
        Export MIME type if available, None otherwise
    """
    return EXPORT_FORMATS.get(mime_type)


def get_file_category(mime_type: str) -> str:
    """
    Categorize file based on MIME type.

    Args:
        mime_type: MIME type string

    Returns:
        Category string (document, spreadsheet, image, video, etc.)
        Returns 'other' if no category matches
    """
    # Check direct mapping first
    if mime_type in CATEGORY_MAPPINGS:
        return CATEGORY_MAPPINGS[mime_type]

    # Try to infer from MIME type prefix
    if mime_type.startswith("image/"):
        return "image"
    elif mime_type.startswith("video/"):
        return "video"
    elif mime_type.startswith("audio/"):
        return "audio"
    elif mime_type.startswith("text/"):
        return "document"
    elif mime_type.startswith("application/vnd.google-apps."):
        # Catch any unmapped Google Workspace types
        return "google-workspace"

    return "other"


def needs_export(mime_type: str) -> bool:
    """
    Check if a Google Workspace file needs to be exported for content access.

    Args:
        mime_type: MIME type string

    Returns:
        True if the file needs export, False otherwise
    """
    return mime_type in EXPORT_FORMATS


def get_file_extension(mime_type: str) -> Optional[str]:
    """
    Get the typical file extension for a MIME type.

    Args:
        mime_type: MIME type string

    Returns:
        File extension with dot (e.g., '.pdf'), None if unknown
    """
    extension_map = {
        # Google Workspace (when exported)
        GOOGLE_DOC: ".txt",
        GOOGLE_SHEET: ".csv",
        GOOGLE_SLIDE: ".pptx",
        GOOGLE_DRAWING: ".png",
        GOOGLE_SCRIPT: ".json",

        # Documents
        "application/pdf": ".pdf",
        "text/plain": ".txt",
        "text/html": ".html",
        "text/csv": ".csv",
        "application/rtf": ".rtf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
        "application/msword": ".doc",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.ms-powerpoint": ".ppt",

        # Images
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/svg+xml": ".svg",
        "image/webp": ".webp",
        "image/bmp": ".bmp",
        "image/tiff": ".tiff",

        # Videos
        "video/mp4": ".mp4",
        "video/mpeg": ".mpeg",
        "video/quicktime": ".mov",
        "video/x-msvideo": ".avi",
        "video/webm": ".webm",

        # Audio
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/wav": ".wav",
        "audio/ogg": ".ogg",

        # Archives
        "application/zip": ".zip",
        "application/x-rar-compressed": ".rar",
        "application/x-tar": ".tar",
        "application/gzip": ".gz",
        "application/x-7z-compressed": ".7z",

        # Code
        "text/javascript": ".js",
        "application/javascript": ".js",
        "application/json": ".json",
        "text/x-python": ".py",
        "text/x-java-source": ".java",
        "text/markdown": ".md",
        "text/x-yaml": ".yaml",
        "application/xml": ".xml",
        "text/xml": ".xml",
    }

    return extension_map.get(mime_type)