"""Handler modules for Google Drive worker.

Action-based handlers that process messages by action type, not topic.
"""

from google_drive_worker.handlers.base import BaseActionHandler, BaseIntegrationHandler
from google_drive_worker.handlers.capability import GoogleDriveCapabilityHandler
from google_drive_worker.handlers.content_emitter import ContentIngestEmitter
from google_drive_worker.handlers.fetch import FetchHandler
from google_drive_worker.handlers.init import InitHandler
from google_drive_worker.handlers.teardown import TeardownHandler
from google_drive_worker.handlers.webhook import WebhookHandler, GoogleDriveWebhookHandler
from google_drive_worker.handlers.write import WriteHandler

__all__ = [
    # Base classes
    "BaseActionHandler",
    "BaseIntegrationHandler",  # Legacy alias
    # Action handlers
    "InitHandler",
    "FetchHandler",
    "WriteHandler",
    "TeardownHandler",
    "WebhookHandler",
    "GoogleDriveWebhookHandler",  # Legacy alias
    "GoogleDriveCapabilityHandler",
    "ContentIngestEmitter",
]