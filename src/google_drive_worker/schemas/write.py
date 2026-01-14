"""Write message schemas for Google Drive worker.

Pydantic models for validating and parsing write request payloads.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, validator

# Supported write operations
WriteOperation = Literal[
    "upload_file",
    "update_file",
    "update_metadata",
    "delete_file",
    "copy_file",
    "move_file",
    "create_folder",
    "share_file",
    "unshare_file",
]


def validate_write_payload(operation: WriteOperation, payload: dict[str, Any]) -> dict[str, Any]:
    """Validate write operation payload.

    TODO: Implement validation schemas for each operation type.

    Args:
        operation: The write operation type
        payload: The operation payload

    Returns:
        Validated payload dict

    Raises:
        ValidationError: If payload is invalid for the operation
    """
    # TODO: Implement validation for each operation type
    # For now, just return the payload as-is
    return payload
