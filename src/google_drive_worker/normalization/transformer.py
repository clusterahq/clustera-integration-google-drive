"""Google Drive data transformer for normalizing API responses."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .mime_types import (
    get_export_format,
    get_file_category,
    get_file_extension,
    is_folder,
    is_google_workspace_file,
    needs_export,
)

logger = logging.getLogger(__name__)


class GoogleDriveDataTransformer:
    """Transform Google Drive API responses to normalized format."""

    def __init__(self):
        """Initialize the transformer."""
        self.transformation_version = "1.0.0"

    def transform_file(
        self,
        raw_file: Dict[str, Any],
        connection_id: str,
        customer_id: str,
    ) -> Dict[str, Any]:
        """
        Transform Google Drive file API response to normalized format.

        Args:
            raw_file: Raw file data from Google Drive API
            connection_id: Integration connection ID
            customer_id: Customer ID

        Returns:
            Normalized file data dictionary

        Example input from Google Drive API:
            {
                "id": "1A2B3C4D5E",
                "name": "Q4 Report.docx",
                "mimeType": "application/vnd.google-apps.document",
                "createdTime": "2025-01-15T10:00:00Z",
                "modifiedTime": "2025-01-20T15:30:00Z",
                "size": "12345",
                "parents": ["0B1C2D3E4F"],
                "owners": [{"displayName": "Alice", "emailAddress": "alice@example.com"}],
                "webViewLink": "https://drive.google.com/file/d/...",
                "thumbnailLink": "https://...",
                "permissions": [...],
                "trashed": false,
                "starred": false
            }

        Example output (normalized):
            {
                "id": "1A2B3C4D5E",
                "type": "file",
                "name": "Q4 Report.docx",
                "mime_type": "application/vnd.google-apps.document",
                "category": "document",
                "is_folder": false,
                "is_google_workspace": true,
                "export_format": "application/vnd...",
                "size_bytes": 12345,
                "web_view_link": "https://...",
                "thumbnail_link": "https://...",
                "parent_ids": ["0B1C2D3E4F"],
                "owners": [{"name": "Alice", "email": "alice@example.com"}],
                "trashed": false,
                "starred": false,
                "timestamps": {
                    "created_at": "2025-01-15T10:00:00Z",
                    "modified_at": "2025-01-20T15:30:00Z"
                },
                "source": {
                    "provider": "google-drive",
                    "file_id": "1A2B3C4D5E"
                }
            }
        """
        file_id = raw_file.get("id", "")
        mime_type = raw_file.get("mimeType", "application/octet-stream")

        # Determine file attributes based on MIME type
        is_folder_item = is_folder(mime_type)
        is_workspace = is_google_workspace_file(mime_type)
        category = get_file_category(mime_type)
        export_format = get_export_format(mime_type) if is_workspace else None
        file_extension = get_file_extension(mime_type)

        # Build normalized structure
        normalized = {
            "id": file_id,
            "type": "folder" if is_folder_item else "file",
            "name": raw_file.get("name", "Unnamed"),
            "mime_type": mime_type,
            "category": category,
            "is_folder": is_folder_item,
            "is_google_workspace": is_workspace,
            "needs_export": needs_export(mime_type),
        }

        # Add export format if applicable
        if export_format:
            normalized["export_format"] = export_format

        # Add file extension if determinable
        if file_extension:
            normalized["file_extension"] = file_extension

        # Add size (Google Workspace files don't have size)
        if "size" in raw_file:
            try:
                normalized["size_bytes"] = int(raw_file["size"])
            except (ValueError, TypeError):
                normalized["size_bytes"] = 0
        else:
            normalized["size_bytes"] = None  # Google Workspace files

        # Add web links
        if "webViewLink" in raw_file:
            normalized["web_view_link"] = raw_file["webViewLink"]

        if "webContentLink" in raw_file:
            normalized["web_content_link"] = raw_file["webContentLink"]

        if "thumbnailLink" in raw_file:
            normalized["thumbnail_link"] = raw_file["thumbnailLink"]

        # Add parent folder IDs
        normalized["parent_ids"] = raw_file.get("parents", [])

        # Transform owners
        owners = []
        for owner in raw_file.get("owners", []):
            owners.append({
                "name": owner.get("displayName", "Unknown"),
                "email": owner.get("emailAddress"),
                "photo_link": owner.get("photoLink"),
            })
        normalized["owners"] = owners

        # Transform last modifying user
        if "lastModifyingUser" in raw_file:
            user = raw_file["lastModifyingUser"]
            normalized["last_modified_by"] = {
                "name": user.get("displayName", "Unknown"),
                "email": user.get("emailAddress"),
                "photo_link": user.get("photoLink"),
            }

        # Add file capabilities (what the user can do)
        if "capabilities" in raw_file:
            caps = raw_file["capabilities"]
            normalized["capabilities"] = {
                "can_edit": caps.get("canEdit", False),
                "can_comment": caps.get("canComment", False),
                "can_share": caps.get("canShare", False),
                "can_copy": caps.get("canCopy", False),
                "can_download": caps.get("canDownload", False),
                "can_delete": caps.get("canDelete", False),
                "can_rename": caps.get("canRename", False),
                "can_move": caps.get("canMoveItemWithinDrive", False),
                "can_add_children": caps.get("canAddChildren", False),
            }

        # Add flags
        normalized["trashed"] = raw_file.get("trashed", False)
        normalized["starred"] = raw_file.get("starred", False)
        normalized["explicitly_trashed"] = raw_file.get("explicitlyTrashed", False)

        # Add sharing information
        normalized["shared"] = raw_file.get("shared", False)
        normalized["viewed_by_me"] = raw_file.get("viewedByMe", False)

        # Add timestamps
        timestamps = {}
        if "createdTime" in raw_file:
            timestamps["created_at"] = self._parse_timestamp(raw_file["createdTime"])
        if "modifiedTime" in raw_file:
            timestamps["modified_at"] = self._parse_timestamp(raw_file["modifiedTime"])
        if "viewedByMeTime" in raw_file:
            timestamps["viewed_by_me_at"] = self._parse_timestamp(raw_file["viewedByMeTime"])
        if "sharedWithMeTime" in raw_file:
            timestamps["shared_with_me_at"] = self._parse_timestamp(raw_file["sharedWithMeTime"])
        if "trashedTime" in raw_file:
            timestamps["trashed_at"] = self._parse_timestamp(raw_file["trashedTime"])

        normalized["timestamps"] = timestamps

        # Add quota and storage info if available
        if "quotaBytesUsed" in raw_file:
            try:
                normalized["quota_bytes_used"] = int(raw_file["quotaBytesUsed"])
            except (ValueError, TypeError):
                normalized["quota_bytes_used"] = 0

        # Add version information
        if "version" in raw_file:
            normalized["version"] = raw_file["version"]

        # Add file metadata
        if "md5Checksum" in raw_file:
            normalized["md5_checksum"] = raw_file["md5Checksum"]

        if "sha1Checksum" in raw_file:
            normalized["sha1_checksum"] = raw_file["sha1Checksum"]

        if "sha256Checksum" in raw_file:
            normalized["sha256_checksum"] = raw_file["sha256Checksum"]

        # Add description if present
        if "description" in raw_file:
            normalized["description"] = raw_file["description"]

        # Add folder-specific information
        if is_folder_item and "folderColorRgb" in raw_file:
            normalized["folder_color_rgb"] = raw_file["folderColorRgb"]

        # Add source metadata
        normalized["source"] = {
            "provider": "google-drive",
            "file_id": file_id,
            "connection_id": connection_id,
            "customer_id": customer_id,
            "transformation_version": self.transformation_version,
            "transformed_at": datetime.utcnow().isoformat() + "Z",
        }

        # Add original fields count for debugging
        normalized["_metadata"] = {
            "original_field_count": len(raw_file),
            "has_permissions": "permissions" in raw_file,
            "has_app_properties": "appProperties" in raw_file,
            "has_properties": "properties" in raw_file,
        }

        return normalized

    def transform_revision(
        self,
        raw_revision: Dict[str, Any],
        file_id: str,
        connection_id: str,
        customer_id: str,
    ) -> Dict[str, Any]:
        """
        Transform file revision to normalized format.

        Args:
            raw_revision: Raw revision data from Google Drive API
            file_id: File ID this revision belongs to
            connection_id: Integration connection ID
            customer_id: Customer ID

        Returns:
            Normalized revision data dictionary
        """
        revision_id = raw_revision.get("id", "")

        normalized = {
            "id": revision_id,
            "file_id": file_id,
            "type": "revision",
            "keep_forever": raw_revision.get("keepForever", False),
            "published": raw_revision.get("published", False),
            "publish_auto": raw_revision.get("publishAuto", False),
            "published_outside_domain": raw_revision.get("publishedOutsideDomain", False),
        }

        # Add mime type if present
        if "mimeType" in raw_revision:
            normalized["mime_type"] = raw_revision["mimeType"]

        # Add size if present
        if "size" in raw_revision:
            try:
                normalized["size_bytes"] = int(raw_revision["size"])
            except (ValueError, TypeError):
                normalized["size_bytes"] = 0

        # Add checksums if present
        if "md5Checksum" in raw_revision:
            normalized["md5_checksum"] = raw_revision["md5Checksum"]

        # Add last modifying user
        if "lastModifyingUser" in raw_revision:
            user = raw_revision["lastModifyingUser"]
            normalized["last_modified_by"] = {
                "name": user.get("displayName", "Unknown"),
                "email": user.get("emailAddress"),
                "photo_link": user.get("photoLink"),
            }

        # Add timestamps
        timestamps = {}
        if "modifiedTime" in raw_revision:
            timestamps["modified_at"] = self._parse_timestamp(raw_revision["modifiedTime"])

        normalized["timestamps"] = timestamps

        # Add links
        if "exportLinks" in raw_revision:
            normalized["export_links"] = raw_revision["exportLinks"]

        # Add source metadata
        normalized["source"] = {
            "provider": "google-drive",
            "file_id": file_id,
            "revision_id": revision_id,
            "connection_id": connection_id,
            "customer_id": customer_id,
            "transformation_version": self.transformation_version,
            "transformed_at": datetime.utcnow().isoformat() + "Z",
        }

        return normalized

    def transform_permission(
        self,
        raw_permission: Dict[str, Any],
        file_id: str,
        connection_id: str,
        customer_id: str,
    ) -> Dict[str, Any]:
        """
        Transform permission (sharing) to normalized format.

        Args:
            raw_permission: Raw permission data from Google Drive API
            file_id: File ID this permission belongs to
            connection_id: Integration connection ID
            customer_id: Customer ID

        Returns:
            Normalized permission data dictionary
        """
        permission_id = raw_permission.get("id", "")

        normalized = {
            "id": permission_id,
            "file_id": file_id,
            "type": "permission",
            "permission_type": raw_permission.get("type", ""),  # user, group, domain, anyone
            "role": raw_permission.get("role", ""),  # owner, organizer, fileOrganizer, writer, commenter, reader
            "display_name": raw_permission.get("displayName", ""),
            "email_address": raw_permission.get("emailAddress"),
            "domain": raw_permission.get("domain"),
            "allow_file_discovery": raw_permission.get("allowFileDiscovery", False),
            "deleted": raw_permission.get("deleted", False),
            "pending_owner": raw_permission.get("pendingOwner", False),
        }

        # Add photo link if present
        if "photoLink" in raw_permission:
            normalized["photo_link"] = raw_permission["photoLink"]

        # Add expiration time if present
        if "expirationTime" in raw_permission:
            normalized["expiration_time"] = self._parse_timestamp(raw_permission["expirationTime"])

        # Add permission details if present
        if "permissionDetails" in raw_permission:
            details = []
            for detail in raw_permission["permissionDetails"]:
                details.append({
                    "permission_type": detail.get("permissionType"),
                    "role": detail.get("role"),
                    "inherited_from": detail.get("inheritedFrom"),
                    "inherited": detail.get("inherited", False),
                })
            normalized["permission_details"] = details

        # Add team drive permission details if present
        if "teamDrivePermissionDetails" in raw_permission:
            td_details = []
            for detail in raw_permission["teamDrivePermissionDetails"]:
                td_details.append({
                    "team_drive_permission_type": detail.get("teamDrivePermissionType"),
                    "role": detail.get("role"),
                    "inherited_from": detail.get("inheritedFrom"),
                    "inherited": detail.get("inherited", False),
                })
            normalized["team_drive_permission_details"] = td_details

        # Add source metadata
        normalized["source"] = {
            "provider": "google-drive",
            "file_id": file_id,
            "permission_id": permission_id,
            "connection_id": connection_id,
            "customer_id": customer_id,
            "transformation_version": self.transformation_version,
            "transformed_at": datetime.utcnow().isoformat() + "Z",
        }

        return normalized

    def transform_comment(
        self,
        raw_comment: Dict[str, Any],
        file_id: str,
        connection_id: str,
        customer_id: str,
    ) -> Dict[str, Any]:
        """
        Transform comment to normalized format.

        Args:
            raw_comment: Raw comment data from Google Drive API
            file_id: File ID this comment belongs to
            connection_id: Integration connection ID
            customer_id: Customer ID

        Returns:
            Normalized comment data dictionary
        """
        comment_id = raw_comment.get("id", "")

        normalized = {
            "id": comment_id,
            "file_id": file_id,
            "type": "comment",
            "content": raw_comment.get("content", ""),
            "resolved": raw_comment.get("resolved", False),
            "deleted": raw_comment.get("deleted", False),
        }

        # Add author information
        if "author" in raw_comment:
            author = raw_comment["author"]
            normalized["author"] = {
                "name": author.get("displayName", "Unknown"),
                "email": author.get("emailAddress"),
                "photo_link": author.get("photoLink"),
            }

        # Add timestamps
        timestamps = {}
        if "createdTime" in raw_comment:
            timestamps["created_at"] = self._parse_timestamp(raw_comment["createdTime"])
        if "modifiedTime" in raw_comment:
            timestamps["modified_at"] = self._parse_timestamp(raw_comment["modifiedTime"])

        normalized["timestamps"] = timestamps

        # Add replies if present
        if "replies" in raw_comment:
            replies = []
            for reply in raw_comment["replies"]:
                reply_data = {
                    "id": reply.get("id", ""),
                    "content": reply.get("content", ""),
                    "deleted": reply.get("deleted", False),
                }

                # Add reply author
                if "author" in reply:
                    reply_author = reply["author"]
                    reply_data["author"] = {
                        "name": reply_author.get("displayName", "Unknown"),
                        "email": reply_author.get("emailAddress"),
                        "photo_link": reply_author.get("photoLink"),
                    }

                # Add reply timestamps
                reply_timestamps = {}
                if "createdTime" in reply:
                    reply_timestamps["created_at"] = self._parse_timestamp(reply["createdTime"])
                if "modifiedTime" in reply:
                    reply_timestamps["modified_at"] = self._parse_timestamp(reply["modifiedTime"])
                reply_data["timestamps"] = reply_timestamps

                replies.append(reply_data)

            normalized["replies"] = replies
            normalized["reply_count"] = len(replies)

        # Add source metadata
        normalized["source"] = {
            "provider": "google-drive",
            "file_id": file_id,
            "comment_id": comment_id,
            "connection_id": connection_id,
            "customer_id": customer_id,
            "transformation_version": self.transformation_version,
            "transformed_at": datetime.utcnow().isoformat() + "Z",
        }

        return normalized

    def transform_files_batch(
        self,
        raw_files: List[Dict[str, Any]],
        connection_id: str,
        customer_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Transform a batch of files.

        Args:
            raw_files: List of raw file data from Google Drive API
            connection_id: Integration connection ID
            customer_id: Customer ID

        Returns:
            List of normalized file data dictionaries
        """
        normalized_files = []
        for raw_file in raw_files:
            try:
                normalized = self.transform_file(raw_file, connection_id, customer_id)
                normalized_files.append(normalized)
            except Exception as e:
                logger.error(
                    f"Failed to transform file {raw_file.get('id', 'unknown')}: {e}",
                    exc_info=True,
                )
                # Continue processing other files
                continue

        return normalized_files

    def _parse_timestamp(self, timestamp_str: str) -> str:
        """
        Parse and normalize timestamp string.

        Args:
            timestamp_str: Timestamp string from API

        Returns:
            ISO format timestamp string
        """
        # Google Drive API returns RFC3339 format, which is already ISO-compatible
        # Just ensure it has the Z suffix for UTC
        if timestamp_str and not timestamp_str.endswith("Z"):
            if timestamp_str.endswith("+00:00"):
                timestamp_str = timestamp_str[:-6] + "Z"
        return timestamp_str