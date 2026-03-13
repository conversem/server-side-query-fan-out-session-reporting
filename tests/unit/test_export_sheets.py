"""Unit tests for export_session_report sheet builder functions."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Add scripts to path for import
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from export_session_report import (
    DASHBOARD_VIEW_SHEETS,
    _build_dashboard_sheets,
    _build_sessions_sheet,
)


@pytest.fixture
def mock_backend_sessions():
    """Mock backend returning session-like data for _build_sessions_sheet."""
    mock = MagicMock()
    mock.backend_type = "sqlite"

    sessions_rows = [
        {
            "session_id": "sess-001",
            "fanout_session_name": "test session",
            "session_date": "2025-01-15",
            "session_start_time": "2025-01-15T10:00:00Z",
            "session_end_time": "2025-01-15T10:00:05Z",
            "duration_ms": 5000,
            "bot_provider": "OpenAI",
            "bot_name": "ChatGPT",
            "request_count": 5,
            "unique_urls": 4,
            "mean_cosine_similarity": 0.85,
            "min_cosine_similarity": 0.72,
            "confidence_level": "high",
            "url_list": '["/a", "/b"]',
        }
    ]
    summary_rows = [
        {
            "session_date": "2025-01-15",
            "total_sessions": 1,
            "unique_urls_requested": 4,
            "avg_requests_per_session": 5.0,
            "avg_urls_per_session": 4.0,
            "avg_coherence": 0.85,
            "high_confidence": 1,
            "medium_confidence": 0,
            "low_confidence": 0,
        }
    ]
    top_urls_rows = [
        {"url": "/page/1", "frequency": 3, "session_count": 1, "avg_coherence": 0.8}
    ]
    provider_rows = [
        {
            "bot_provider": "OpenAI",
            "bot_name": "ChatGPT",
            "total_sessions": 1,
            "total_requests": 5,
            "high_confidence_pct": 100.0,
        }
    ]

    mock.query.side_effect = [
        sessions_rows,
        summary_rows,
        top_urls_rows,
        provider_rows,
        [],  # url_details (empty)
    ]
    return mock


@pytest.fixture
def mock_backend_dashboard():
    """Mock backend returning dashboard view data for _build_dashboard_sheets."""
    mock = MagicMock()
    mock.backend_type = "sqlite"

    # One row per dashboard view
    dashboard_row = [
        {"session_date": "2025-01-15", "total_sessions": 10, "url_bucket": "1-5"}
    ]
    mock.query.side_effect = [dashboard_row] * len(DASHBOARD_VIEW_SHEETS)
    return mock


class TestBuildSessionsSheet:
    def test_build_sessions_sheet_returns_none_when_no_sessions(self):
        mock = MagicMock()
        mock.backend_type = "sqlite"
        mock.query.return_value = []

        result = _build_sessions_sheet(mock)

        assert result is None

    def test_build_sessions_sheet_has_expected_headers(self, mock_backend_sessions):
        result = _build_sessions_sheet(mock_backend_sessions)

        assert result is not None
        assert "Sessions" in result
        df = result["Sessions"]
        expected_headers = [
            "session_id",
            "fanout_session_name",
            "session_date",
            "session_start_time",
            "session_end_time",
            "duration_ms",
            "bot_provider",
            "bot_name",
            "request_count",
            "unique_urls",
            "mean_cosine_similarity",
            "min_cosine_similarity",
            "confidence_level",
            "url_list",
        ]
        for h in expected_headers:
            assert h in df.columns, f"Missing column: {h}"

    def test_build_sessions_sheet_includes_summary_top_urls_provider(
        self, mock_backend_sessions
    ):
        result = _build_sessions_sheet(mock_backend_sessions)

        assert "Summary" in result
        assert "session_date" in result["Summary"].columns
        assert "total_sessions" in result["Summary"].columns

        assert "Top URLs" in result
        assert "url" in result["Top URLs"].columns
        assert "frequency" in result["Top URLs"].columns

        assert "Provider Stats" in result
        assert "bot_provider" in result["Provider Stats"].columns


class TestBuildDashboardSheets:
    def test_build_dashboard_sheets_returns_dict(self, mock_backend_dashboard):
        result = _build_dashboard_sheets(mock_backend_dashboard)

        assert isinstance(result, dict)

    def test_build_dashboard_sheets_has_expected_headers(self, mock_backend_dashboard):
        result = _build_dashboard_sheets(mock_backend_dashboard)

        for sheet_name, df in result.items():
            assert not df.empty, f"Sheet {sheet_name} should not be empty"
            assert len(df.columns) > 0, f"Sheet {sheet_name} should have columns"
            # At least one of the common dashboard columns
            assert any(
                c in df.columns
                for c in ["session_date", "total_sessions", "url_bucket"]
            ), f"Sheet {sheet_name} missing expected columns"
