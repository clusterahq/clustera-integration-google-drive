"""Capability handler for Google Drive worker.

Handles capability discovery requests and returns the Google Drive worker's
capability manifest.
"""

from clustera_integration_toolkit.capability import CapabilityHandler, CapabilityManifest

from google_drive_worker.capabilities import GOOGLE_DRIVE_CAPABILITIES


class GoogleDriveCapabilityHandler(CapabilityHandler):
    """Handler for Google Drive worker capability discovery.

    Processes clustera.integration.capability.describe requests and returns
    the complete Google Drive capability manifest.
    """

    def get_capability_manifest(self) -> CapabilityManifest:
        """Return the Google Drive worker's capability manifest.

        Returns:
            CapabilityManifest describing all supported Google Drive operations.
        """
        return GOOGLE_DRIVE_CAPABILITIES
