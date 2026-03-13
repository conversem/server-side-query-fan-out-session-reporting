"""Integration tests for LocalPipeline dual-backend support."""

import sqlite3
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline
from llm_bot_pipeline.storage import get_backend


def _seed_raw_data(db_path: Path) -> None:
    """Insert minimal raw_bot_requests rows for testing."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_bot_requests (
            EdgeStartTimestamp TEXT,
            ClientRequestURI TEXT,
            ClientRequestHost TEXT,
            domain TEXT,
            ClientRequestUserAgent TEXT,
            BotScore INTEGER,
            VerifiedBot INTEGER,
            ClientIP TEXT,
            ClientCountry TEXT,
            EdgeResponseStatus INTEGER
        )
    """)
    rows = [
        (
            "2026-01-15T10:00:00Z",
            "/page/1",
            "example.com",
            None,
            "Mozilla/5.0 (compatible; GPTBot/1.0)",
            10,
            1,
            "1.2.3.4",
            "US",
            200,
        ),
        (
            "2026-01-15T11:00:00Z",
            "/page/2",
            "example.com",
            None,
            "Mozilla/5.0 (compatible; ClaudeBot/1.0)",
            15,
            1,
            "5.6.7.8",
            "NL",
            200,
        ),
    ]
    conn.executemany("INSERT INTO raw_bot_requests VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()


@pytest.fixture()
def raw_backend(tmp_path):
    """SQLite backend with seeded raw data."""
    db_path = tmp_path / "raw.db"
    _seed_raw_data(db_path)
    backend = get_backend("sqlite", db_path=db_path)
    backend.initialize()
    yield backend
    backend.close()


@pytest.fixture()
def output_backend(tmp_path):
    """Separate SQLite backend acting as the output backend."""
    db_path = tmp_path / "output.db"
    backend = get_backend("sqlite", db_path=db_path)
    backend.initialize()
    yield backend
    backend.close()


class TestDualBackendOutput:
    def test_output_backend_receives_clean_records(self, raw_backend, output_backend):
        pipeline = LocalPipeline(backend=raw_backend, output_backend=output_backend)
        result = pipeline.run(
            start_date=date(2026, 1, 15),
            end_date=date(2026, 1, 15),
            mode="full",
        )
        assert result.success
        assert result.transformed_rows > 0

        rows = output_backend.query("SELECT COUNT(*) as cnt FROM bot_requests_daily")
        assert rows[0]["cnt"] > 0

    def test_raw_backend_clean_table_empty(self, raw_backend, output_backend):
        """Clean records go to output_backend, not raw_backend."""
        pipeline = LocalPipeline(backend=raw_backend, output_backend=output_backend)
        pipeline.run(
            start_date=date(2026, 1, 15),
            end_date=date(2026, 1, 15),
            mode="full",
        )
        raw_clean = raw_backend.query("SELECT COUNT(*) as cnt FROM bot_requests_daily")
        assert raw_clean[0]["cnt"] == 0


class TestDefaultOutputBackend:
    def test_default_output_backend_is_raw(self, raw_backend):
        pipeline = LocalPipeline(backend=raw_backend)
        assert pipeline._output_backend is pipeline._backend


class TestPipelineStatus:
    def test_get_pipeline_status_dual_backend(self, raw_backend, output_backend):
        pipeline = LocalPipeline(backend=raw_backend, output_backend=output_backend)
        pipeline.run(
            start_date=date(2026, 1, 15),
            end_date=date(2026, 1, 15),
            mode="full",
        )
        status = pipeline.get_pipeline_status()
        assert status["raw_backend_type"] == "sqlite"
        assert status["output_backend_type"] == "sqlite"
        assert status["raw_table_exists"] is True


class TestClosesBothBackends:
    def test_close_closes_both(self):
        raw = MagicMock()
        raw.backend_type = "sqlite"
        out = MagicMock()
        out.backend_type = "bigquery"

        pipeline = LocalPipeline.__new__(LocalPipeline)
        pipeline._backend = raw
        pipeline._output_backend = out
        pipeline._owns_backend = True
        pipeline._owns_output_backend = True
        pipeline._backend_type = "sqlite"
        pipeline._initialized = True

        pipeline.close()

        raw.close.assert_called_once()
        out.close.assert_called_once()
