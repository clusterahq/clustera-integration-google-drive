"""Entry point for the Google Drive worker.

Run with: python -m google_drive_worker

Required environment variables:
    CONTROL_PLANE_BASE_URL: Control Plane API endpoint
    CONTROL_PLANE_M2M_TOKEN: M2M token with read:integration-providers and read:secrets scopes
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from typing import NoReturn

import structlog

from clustera_integration_toolkit.bootstrap import (
    bootstrap_worker_environment,
    EnvironmentBootstrapError,
    InsufficientPermissionsError,
    MissingEnvironmentError,
)
from google_drive_worker.config import Settings
from google_drive_worker.health import HealthServer
from google_drive_worker.worker import GoogleDriveWorker


async def main() -> NoReturn:
    """Main entry point for the Google Drive worker."""
    # Bootstrap environment from Control Plane before loading settings
    # The bootstrap response includes provider_name needed for external ID resolution
    try:
        provider_env = await bootstrap_worker_environment("google-drive")
    except (MissingEnvironmentError, InsufficientPermissionsError) as e:
        # Print the detailed error message directly (it's pre-formatted)
        print(str(e), file=sys.stderr)
        sys.exit(1)
    except EnvironmentBootstrapError as e:
        print(f"\nBootstrap failed: {e}", file=sys.stderr)
        sys.exit(1)

    # Extract provider_name for connection resolution via external ID
    provider_name = provider_env.provider_name

    settings = Settings()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
            if settings.logging.format == "console"
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.logging.level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logger = structlog.get_logger()
    logger.info(
        "Starting Google Drive worker",
        version="0.1.0",
        integration_id=settings.worker.integration_id,
        mock_mode=settings.google_drive.mock_mode,
    )

    worker = GoogleDriveWorker(settings, provider_name=provider_name)

    # Start health check server
    health_port = int(os.environ.get("HEALTH_PORT", "8080"))
    health_server = HealthServer(worker, port=health_port)

    try:
        await health_server.start()
        logger.info("Health server started", port=health_port)
    except Exception as e:
        logger.warning("Failed to start health server", error=str(e))
        health_server = None  # type: ignore

    loop = asyncio.get_running_loop()

    async def shutdown() -> None:
        """Graceful shutdown."""
        await worker.stop()
        if health_server:
            await health_server.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown()))

    try:
        await worker.start()
    except Exception as e:
        logger.exception("Worker failed with exception", error=str(e))
        sys.exit(1)
    finally:
        if health_server:
            await health_server.stop()

    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())