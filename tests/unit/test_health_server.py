"""Unit tests for the health server endpoint.

Tests use direct handler invocation instead of a real HTTP server to avoid
threading, real sockets, and freezegun interactions with select().
"""

import json
from http import HTTPStatus
from unittest.mock import MagicMock, call, patch

import pytest

from llm_bot_pipeline.health_server import _make_handler


def _invoke_handler(backend, path: str) -> tuple[int, dict]:
    """Invoke the health handler for a given path and return (status_code, body_dict).

    Creates a bound handler class from _make_handler, instantiates it with
    mocked HTTP internals, calls do_GET(), then captures the response written
    to wfile.
    """
    handler_cls = _make_handler(backend)

    handler = handler_cls.__new__(handler_cls)
    handler.path = path

    written_chunks = []
    handler.wfile = MagicMock()
    handler.wfile.write.side_effect = written_chunks.append
    handler.send_response = MagicMock()
    handler.send_header = MagicMock()
    handler.end_headers = MagicMock()

    handler.do_GET()

    status_code = handler.send_response.call_args[0][0]
    body = json.loads(b"".join(written_chunks).decode("utf-8"))
    return status_code, body


class TestHealthEndpointHealthy:
    def test_returns_200_when_backend_healthy(self):
        mock_backend = MagicMock()
        mock_backend.health_check.return_value = {
            "healthy": True,
            "backend_type": "sqlite",
            "message": "Backend is operational",
            "details": {},
        }

        status, body = _invoke_handler(mock_backend, "/health")

        assert status == HTTPStatus.OK
        assert body["status"] == "healthy"
        assert body["healthy"] is True
        mock_backend.health_check.assert_called_once()

    def test_readiness_returns_200_when_healthy(self):
        mock_backend = MagicMock()
        mock_backend.health_check.return_value = {
            "healthy": True,
            "backend_type": "sqlite",
            "message": "OK",
            "details": {},
        }

        status, body = _invoke_handler(mock_backend, "/readiness")

        assert status == HTTPStatus.OK
        assert body["status"] == "healthy"


class TestHealthEndpointUnhealthy:
    def test_returns_503_when_backend_unhealthy(self):
        mock_backend = MagicMock()
        mock_backend.health_check.return_value = {
            "healthy": False,
            "backend_type": "bigquery",
            "message": "Connection refused",
            "details": {"error": "Connection refused"},
        }

        status, body = _invoke_handler(mock_backend, "/health")

        assert status == HTTPStatus.SERVICE_UNAVAILABLE
        assert body["status"] == "unhealthy"
        assert "Connection refused" in body["error"]

    def test_returns_503_when_health_check_raises(self):
        mock_backend = MagicMock()
        mock_backend.health_check.side_effect = RuntimeError("DB down")

        status, body = _invoke_handler(mock_backend, "/health")

        assert status == HTTPStatus.SERVICE_UNAVAILABLE
        assert body["status"] == "unhealthy"
        assert "DB down" in body["error"]

    def test_returns_503_when_no_backend(self):
        status, body = _invoke_handler(None, "/health")

        assert status == HTTPStatus.SERVICE_UNAVAILABLE
        assert body["status"] == "unhealthy"
        assert "no storage backend" in body["error"]


class TestNotFound:
    def test_unknown_path_returns_404(self):
        mock_backend = MagicMock()

        status, body = _invoke_handler(mock_backend, "/unknown")

        assert status == HTTPStatus.NOT_FOUND
        assert body["error"] == "not found"
