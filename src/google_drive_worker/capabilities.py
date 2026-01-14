"""Google Drive worker capability definitions.

This module defines the complete capability manifest for the Google Drive integration worker,
documenting all supported actions and operations.
"""

from clustera_integration_toolkit.capability import ManifestBuilder
from clustera_integration_toolkit.capability.builder import OperationBuilder

from google_drive_worker import __version__


# Google Drive worker capability manifest
GOOGLE_DRIVE_CAPABILITIES = (
    ManifestBuilder(
        provider="google-drive",
        worker_version=f"google-drive-worker-v{__version__}",
    )
    .add_action(
        "init",
        description="Initialize Google Drive connection and set up webhooks",
        operations={
            "initialize": OperationBuilder(
                "Validate OAuth credentials and configure Drive API push notifications"
            )
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "setup_webhook": {
                        "type": "boolean",
                        "description": "Whether to set up change notifications",
                        "default": True,
                    },
                    "watch_all_drives": {
                        "type": "boolean",
                        "description": "Watch changes in all drives (My Drive + shared drives)",
                        "default": False,
                    },
                    "resource_types": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["file", "folder", "permission", "revision"]},
                        "description": "Resource types to monitor for changes",
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["success", "error"]},
                    "webhook_configured": {"type": "boolean"},
                    "channel_id": {"type": "string"},
                    "channel_expiration": {"type": "string", "format": "date-time"},
                    "start_page_token": {"type": "string"},
                },
            })
            .with_rate_limit_cost(1)
            .with_estimated_latency(500)
            .build(),
        },
    )
    .add_action(
        "fetch",
        description="Fetch Google Drive resources with pagination and filtering",
        operations={
            "files": OperationBuilder("List files with pagination and filtering")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "filters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Drive API search query (e.g., \"mimeType='application/pdf' and trashed=false\")",
                            },
                            "folder_id": {
                                "type": "string",
                                "description": "Filter by parent folder ID",
                            },
                            "mime_type": {
                                "type": "string",
                                "description": "Filter by MIME type",
                            },
                            "trashed": {
                                "type": "boolean",
                                "description": "Include trashed files",
                                "default": False,
                            },
                        },
                    },
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "page_token": {"type": "string"},
                            "page_size": {"type": "integer", "default": 100, "maximum": 1000},
                        },
                    },
                    "fetch_options": {
                        "type": "object",
                        "properties": {
                            "include_permissions": {"type": "boolean", "default": False},
                            "include_content": {"type": "boolean", "default": False},
                            "fields": {
                                "type": "string",
                                "description": "Comma-separated list of fields to return",
                            },
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "next_page_token": {"type": "string"},
                            "has_more": {"type": "boolean"},
                        },
                    },
                },
            })
            .with_pagination()
            .with_filtering()
            .with_rate_limit_cost(1)
            .with_estimated_latency(300)
            .build(),
            "file": OperationBuilder("Fetch a single file by ID")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["id"],
                "properties": {
                    "filters": {
                        "type": "object",
                        "required": ["id"],
                        "properties": {
                            "id": {"type": "string", "description": "File ID"},
                        },
                    },
                    "fetch_options": {
                        "type": "object",
                        "properties": {
                            "include_permissions": {"type": "boolean", "default": False},
                            "include_content": {"type": "boolean", "default": False},
                            "fields": {
                                "type": "string",
                                "description": "Comma-separated list of fields to return",
                            },
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array", "maxItems": 1},
                },
            })
            .with_rate_limit_cost(1)
            .with_estimated_latency(200)
            .build(),
            "folders": OperationBuilder("List folders with pagination")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "filters": {
                        "type": "object",
                        "properties": {
                            "parent_folder_id": {"type": "string", "description": "Parent folder ID"},
                            "query": {"type": "string", "description": "Drive API search query"},
                        },
                    },
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "page_token": {"type": "string"},
                            "page_size": {"type": "integer", "default": 100, "maximum": 1000},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "next_page_token": {"type": "string"},
                            "has_more": {"type": "boolean"},
                        },
                    },
                },
            })
            .with_pagination()
            .with_filtering()
            .with_rate_limit_cost(1)
            .with_estimated_latency(300)
            .build(),
            "folder": OperationBuilder("Fetch a single folder by ID")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["id"],
                "properties": {
                    "filters": {
                        "type": "object",
                        "required": ["id"],
                        "properties": {
                            "id": {"type": "string", "description": "Folder ID"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array", "maxItems": 1},
                },
            })
            .with_rate_limit_cost(1)
            .with_estimated_latency(200)
            .build(),
            "permissions": OperationBuilder("List permissions for a file or folder")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["file_id"],
                "properties": {
                    "filters": {
                        "type": "object",
                        "required": ["file_id"],
                        "properties": {
                            "file_id": {"type": "string", "description": "File or folder ID"},
                        },
                    },
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "page_token": {"type": "string"},
                            "page_size": {"type": "integer", "default": 100},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "next_page_token": {"type": "string"},
                            "has_more": {"type": "boolean"},
                        },
                    },
                },
            })
            .with_pagination()
            .with_rate_limit_cost(1)
            .with_estimated_latency(200)
            .build(),
            "permission": OperationBuilder("Fetch a single permission by ID")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["file_id", "permission_id"],
                "properties": {
                    "filters": {
                        "type": "object",
                        "required": ["file_id", "permission_id"],
                        "properties": {
                            "file_id": {"type": "string", "description": "File or folder ID"},
                            "permission_id": {"type": "string", "description": "Permission ID"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array", "maxItems": 1},
                },
            })
            .with_rate_limit_cost(1)
            .with_estimated_latency(150)
            .build(),
            "revisions": OperationBuilder("List revisions for a file")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["file_id"],
                "properties": {
                    "filters": {
                        "type": "object",
                        "required": ["file_id"],
                        "properties": {
                            "file_id": {"type": "string", "description": "File ID"},
                        },
                    },
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "page_token": {"type": "string"},
                            "page_size": {"type": "integer", "default": 100, "maximum": 1000},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "next_page_token": {"type": "string"},
                            "has_more": {"type": "boolean"},
                        },
                    },
                },
            })
            .with_pagination()
            .with_rate_limit_cost(1)
            .with_estimated_latency(250)
            .build(),
            "revision": OperationBuilder("Fetch a single revision by ID")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["file_id", "revision_id"],
                "properties": {
                    "filters": {
                        "type": "object",
                        "required": ["file_id", "revision_id"],
                        "properties": {
                            "file_id": {"type": "string", "description": "File ID"},
                            "revision_id": {"type": "string", "description": "Revision ID"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array", "maxItems": 1},
                },
            })
            .with_rate_limit_cost(1)
            .with_estimated_latency(200)
            .build(),
            "changes": OperationBuilder("Fetch incremental changes since a page token")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["page_token"],
                "properties": {
                    "filters": {
                        "type": "object",
                        "required": ["page_token"],
                        "properties": {
                            "page_token": {
                                "type": "string",
                                "description": "Page token to start from (from changes.getStartPageToken)",
                            },
                            "include_removed": {
                                "type": "boolean",
                                "description": "Include removed/trashed files",
                                "default": True,
                            },
                            "restrict_to_my_drive": {
                                "type": "boolean",
                                "description": "Restrict to My Drive only",
                                "default": False,
                            },
                        },
                    },
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "page_size": {"type": "integer", "default": 100, "maximum": 1000},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "next_page_token": {"type": "string"},
                            "has_more": {"type": "boolean"},
                            "new_start_page_token": {"type": "string"},
                        },
                    },
                },
            })
            .with_pagination()
            .with_rate_limit_cost(1)
            .with_estimated_latency(300)
            .build(),
            "about": OperationBuilder("Fetch user account information and Drive storage quota")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {},
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array", "maxItems": 1},
                },
            })
            .with_rate_limit_cost(1)
            .with_estimated_latency(150)
            .build(),
        },
    )
    .add_action(
        "write",
        description="Perform write operations on Google Drive (upload, share, manage)",
        operations={
            "create_file": OperationBuilder("Upload a new file to Drive")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["name"],
                        "properties": {
                            "name": {"type": "string", "description": "File name"},
                            "mime_type": {"type": "string", "description": "MIME type"},
                            "parent_folder_id": {"type": "string", "description": "Parent folder ID"},
                            "content": {"type": "string", "description": "File content (base64 or text)"},
                            "description": {"type": "string"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(5)
            .with_estimated_latency(500)
            .build(),
            "update_file": OperationBuilder("Update file metadata or content")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["file_id"],
                        "properties": {
                            "file_id": {"type": "string"},
                            "name": {"type": "string"},
                            "description": {"type": "string"},
                            "content": {"type": "string", "description": "Updated file content"},
                            "mime_type": {"type": "string"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(5)
            .with_estimated_latency(400)
            .build(),
            "delete_file": OperationBuilder("Permanently delete a file")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["file_id"],
                        "properties": {
                            "file_id": {"type": "string"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(2)
            .with_estimated_latency(300)
            .build(),
            "trash_file": OperationBuilder("Move a file to trash")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["file_id"],
                        "properties": {
                            "file_id": {"type": "string"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(2)
            .with_estimated_latency(250)
            .build(),
            "untrash_file": OperationBuilder("Restore a file from trash")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["file_id"],
                        "properties": {
                            "file_id": {"type": "string"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(2)
            .with_estimated_latency(250)
            .build(),
            "create_folder": OperationBuilder("Create a new folder")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["name"],
                        "properties": {
                            "name": {"type": "string"},
                            "parent_folder_id": {"type": "string"},
                            "description": {"type": "string"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(2)
            .with_estimated_latency(300)
            .build(),
            "copy_file": OperationBuilder("Create a copy of a file")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["file_id"],
                        "properties": {
                            "file_id": {"type": "string"},
                            "name": {"type": "string", "description": "Name for the copy"},
                            "parent_folder_id": {"type": "string"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(5)
            .with_estimated_latency(500)
            .build(),
            "move_file": OperationBuilder("Move a file to a different folder")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["file_id", "new_parent_folder_id"],
                        "properties": {
                            "file_id": {"type": "string"},
                            "new_parent_folder_id": {"type": "string"},
                            "remove_parents": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Parent folder IDs to remove",
                            },
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(2)
            .with_estimated_latency(300)
            .build(),
            "create_permission": OperationBuilder("Grant permission to a file or folder")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["file_id", "role", "type"],
                        "properties": {
                            "file_id": {"type": "string"},
                            "role": {
                                "type": "string",
                                "enum": ["owner", "organizer", "fileOrganizer", "writer", "commenter", "reader"],
                            },
                            "type": {
                                "type": "string",
                                "enum": ["user", "group", "domain", "anyone"],
                            },
                            "email_address": {"type": "string"},
                            "domain": {"type": "string"},
                            "send_notification_email": {"type": "boolean", "default": True},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(3)
            .with_estimated_latency(400)
            .build(),
            "update_permission": OperationBuilder("Update an existing permission")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["file_id", "permission_id", "role"],
                        "properties": {
                            "file_id": {"type": "string"},
                            "permission_id": {"type": "string"},
                            "role": {
                                "type": "string",
                                "enum": ["owner", "organizer", "fileOrganizer", "writer", "commenter", "reader"],
                            },
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(3)
            .with_estimated_latency(300)
            .build(),
            "delete_permission": OperationBuilder("Remove permission from a file or folder")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "required": ["file_id", "permission_id"],
                        "properties": {
                            "file_id": {"type": "string"},
                            "permission_id": {"type": "string"},
                        },
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(2)
            .with_estimated_latency(250)
            .build(),
        },
    )
    .add_action(
        "webhook",
        description="Parse Google Drive push notification (change notification)",
        operations={
            "parse_notification": OperationBuilder("Parse Drive API push notification payload")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["payload"],
                "properties": {
                    "payload": {
                        "type": "object",
                        "description": "Drive API push notification headers and body",
                    },
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "records": {"type": "array"},
                },
            })
            .with_rate_limit_cost(0)
            .with_estimated_latency(50)
            .build(),
        },
    )
    .add_action(
        "teardown",
        description="Stop push notifications and clean up Google Drive connection",
        operations={
            "stop_channel": OperationBuilder("Stop Drive API push notifications")
            .with_parameters({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "channel_id": {"type": "string", "description": "Channel ID to stop"},
                    "resource_id": {"type": "string", "description": "Resource ID from watch response"},
                },
            })
            .with_response({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["success", "error"]},
                    "channel_stopped": {"type": "boolean"},
                },
            })
            .with_rate_limit_cost(1)
            .with_estimated_latency(200)
            .build(),
        },
    )
    .build()
)
