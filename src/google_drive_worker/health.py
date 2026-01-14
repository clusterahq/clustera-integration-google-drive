"""Health check server for Google Drive worker.

Provides HTTP health endpoints for Kubernetes probes and monitoring.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from google_drive_worker.worker import GoogleDriveWorker


class HealthServer:
    """HTTP server for health checks.

    Provides endpoints:
    - /health: Liveness probe (worker is running)
    - /ready: Readiness probe (worker is ready to process)
    - /metrics: Basic metrics in JSON format
    """

    def __init__(
        self,
        worker: GoogleDriveWorker,
        host: str = "0.0.0.0",
        port: int = 8080,
    ) -> None:
        """Initialize health server.

        Args:
            worker: The Google Drive worker instance
            host: Host to bind to
            port: Port to listen on
        """
        self.worker = worker
        self.host = host
        self.port = port
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        """Start the health server."""
        self._app = web.Application()
        self._app.router.add_get("/health", self._health_handler)
        self._app.router.add_get("/ready", self._ready_handler)
        self._app.router.add_get("/metrics", self._metrics_handler)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()

    async def stop(self) -> None:
        """Stop the health server."""
        if self._runner:
            await self._runner.cleanup()

    async def _health_handler(self, request: web.Request) -> web.Response:
        """Handle /health endpoint (liveness probe).

        Returns 200 if the worker process is alive.
        """
        return web.json_response(
            {
                "status": "healthy",
                "worker_running": self.worker.running,
            }
        )

    async def _ready_handler(self, request: web.Request) -> web.Response:
        """Handle /ready endpoint (readiness probe).

        Returns 200 if the worker is ready to process messages.
        """
        is_ready = (
            self.worker.running
            and self.worker.consumer is not None
            and self.worker.producer is not None
        )

        if is_ready:
            return web.json_response(
                {
                    "status": "ready",
                    "kafka_consumer": "connected",
                    "kafka_producer": "connected",
                }
            )
        else:
            return web.json_response(
                {
                    "status": "not_ready",
                    "kafka_consumer": "connected" if self.worker.consumer else "disconnected",
                    "kafka_producer": "connected" if self.worker.producer else "disconnected",
                },
                status=503,
            )

    async def _metrics_handler(self, request: web.Request) -> web.Response:
        """Handle /metrics endpoint.

        Returns worker metrics in JSON format.
        """
        return web.json_response(
            {
                "metrics": self.worker.metrics,
                "active_connections": len(self.worker.active_connections),
                "integration_id": self.worker.settings.worker.integration_id,
                "mock_mode": self.worker.settings.google_drive.mock_mode,
            }
        )


def create_health_check_response(worker: GoogleDriveWorker) -> dict[str, Any]:
    """Create health check response data.

    Args:
        worker: The Google Drive worker instance

    Returns:
        Dict with health status information
    """
    return {
        "status": "healthy" if worker.running else "unhealthy",
        "worker_running": worker.running,
        "kafka_consumer": "connected" if worker.consumer else "disconnected",
        "kafka_producer": "connected" if worker.producer else "disconnected",
        "metrics": {
            "messages_processed": worker.metrics.get("messages_processed", 0),
            "records_produced": worker.metrics.get("records_produced", 0),
            "errors_produced": worker.metrics.get("errors_produced", 0),
        },
        "active_connections": len(worker.active_connections),
    }
