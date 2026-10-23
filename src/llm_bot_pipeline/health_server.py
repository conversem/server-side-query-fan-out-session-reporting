"""
Lightweight HTTP health endpoint for Cloud Run health checks.

Uses Python's built-in http.server to avoid extra dependencies.
Validates storage backend connectivity via backend.health_check().

Configuration via environment variables:
    HEALTH_PORT  – port to listen on (default 8080)
    HEALTH_HOST  – bind address (default 0.0.0.0)

Usage:
    # As a module
    python -m llm_bot_pipeline.health_server

    # Programmatically
    from llm_bot_pipeline.health_server import start_health_server
    start_health_server(backend)
"""

import json
import logging
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional, Type

from .storage.base import StorageBackend

logger = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8080


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP request handler that serves /health and /readiness endpoints."""

    backend: Optional[StorageBackend] = None

    def do_GET(self):
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/readiness":
            self._handle_health()
        else:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def _handle_health(self):
        if self.backend is None:
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"status": "unhealthy", "error": "no storage backend configured"},
            )
            return

        try:
            result = self.backend.health_check()
        except Exception as e:
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"status": "unhealthy", "error": str(e)},
            )
            return

        if result.get("healthy"):
            self._send_json(HTTPStatus.OK, {"status": "healthy", **result})
        else:
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {
                    "status": "unhealthy",
                    "error": result.get("message", "unknown failure"),
                    **result,
                },
            )

    def _send_json(self, status: HTTPStatus, body: dict):
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        logger.debug("health_server: %s", format % args)


def _make_handler(backend: Optional[StorageBackend]) -> Type[HealthHandler]:
    """Create a handler class bound to a specific backend instance."""

    class BoundHealthHandler(HealthHandler):
        pass

    BoundHealthHandler.backend = backend
    return BoundHealthHandler


def start_health_server(
    backend: Optional[StorageBackend] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    *,
    threaded: bool = True,
) -> HTTPServer:
    """Start the health check HTTP server.

    Args:
        backend: StorageBackend to check connectivity against.
        host: Bind address (falls back to HEALTH_HOST env var, then 0.0.0.0).
        port: Listen port (falls back to HEALTH_PORT env var, then 8080).
        threaded: If True, run server in a daemon thread (non-blocking).

    Returns:
        The HTTPServer instance (call .shutdown() to stop).
    """
    host = host if host is not None else os.environ.get("HEALTH_HOST", DEFAULT_HOST)
    port = (
        port
        if port is not None
        else int(os.environ.get("HEALTH_PORT", str(DEFAULT_PORT)))
    )

    handler_cls = _make_handler(backend)
    server = HTTPServer((host, port), handler_cls)

    if threaded:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logger.info("Health server started on %s:%d (daemon thread)", host, port)
    else:
        logger.info("Health server starting on %s:%d (blocking)", host, port)
        server.serve_forever()

    return server


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    from .storage.factory import get_backend

    try:
        backend = get_backend()
        backend.initialize()
    except Exception as e:
        logger.warning("Could not initialize storage backend: %s", e)
        backend = None

    start_health_server(backend, threaded=False)
