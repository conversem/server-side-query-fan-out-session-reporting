"""
Integration tests for session aggregation in the daily pipeline.

Tests:
- Session aggregation runs after successful ETL
- Session aggregation skipped when --skip-sessions flag used
- Session aggregation handles empty data gracefully
- Pipeline returns correct status including session metrics
"""

import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

# Add src and scripts to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from llm_bot_pipeline.config import OPTIMAL_WINDOW_MS
from llm_bot_pipeline.storage import get_backend


@pytest.fixture
def db_with_raw_data(tmp_path):
    """Create database with raw bot request data for testing."""
    db_path = tmp_path / "test_pipeline.db"
    backend = get_backend(backend_type="sqlite", db_path=db_path)
    backend.initialize()

    # Create query_fanout_sessions table
    create_sessions_sql = """
        CREATE TABLE IF NOT EXISTS query_fanout_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL UNIQUE,
            session_date TEXT NOT NULL,
            session_start_time TEXT NOT NULL,
            session_end_time TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            bot_provider TEXT NOT NULL,
            bot_name TEXT,
            request_count INTEGER NOT NULL,
            unique_urls INTEGER NOT NULL,
            mean_cosine_similarity REAL,
            min_cosine_similarity REAL,
            max_cosine_similarity REAL,
            confidence_level TEXT NOT NULL,
            fanout_session_name TEXT,
            url_list TEXT NOT NULL,
            window_ms REAL NOT NULL,
            _created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """
    backend.execute(create_sessions_sql)

    # Insert raw data that will be transformed
    base_time = datetime(2025, 1, 15, 10, 0, 0)
    raw_records = []

    # Create a cluster of user_request traffic (should become a session)
    for i in range(5):
        record = {
            "EdgeStartTimestamp": (
                base_time + timedelta(milliseconds=i * 20)
            ).isoformat()
            + "Z",
            "ClientRequestURI": f"/mortgage/page-{i}",
            "ClientRequestHost": "example.com",
            "ClientRequestUserAgent": "Mozilla/5.0 ChatGPT-User",
            "BotScore": 1,
            "BotScoreSrc": "Verified Bot",
            "VerifiedBot": 1,
            "BotTags": json.dumps(["ChatGPT-User"]),
            "ClientIP": "1.2.3.4",
            "ClientCountry": "US",
            "EdgeResponseStatus": 200,
            "_ingestion_time": datetime.now().isoformat(),
        }
        raw_records.append(record)

    # Create another cluster 5 seconds later
    for i in range(3):
        record = {
            "EdgeStartTimestamp": (
                base_time + timedelta(seconds=5, milliseconds=i * 30)
            ).isoformat()
            + "Z",
            "ClientRequestURI": f"/insurance/policy-{i}",
            "ClientRequestHost": "example.com",
            "ClientRequestUserAgent": "Mozilla/5.0 PerplexityBot",
            "BotScore": 1,
            "BotScoreSrc": "Verified Bot",
            "VerifiedBot": 1,
            "BotTags": json.dumps(["PerplexityBot"]),
            "ClientIP": "5.6.7.8",
            "ClientCountry": "NL",
            "EdgeResponseStatus": 200,
            "_ingestion_time": datetime.now().isoformat(),
        }
        raw_records.append(record)

    # Insert raw records
    for record in raw_records:
        columns = ", ".join(record.keys())
        placeholders = ", ".join(f":{k}" for k in record.keys())
        sql = f"INSERT INTO raw_bot_requests ({columns}) VALUES ({placeholders})"
        backend.execute(sql, record)

    yield db_path, backend

    backend.close()


@pytest.fixture
def db_with_processed_data(db_with_raw_data):
    """Create database with already-processed data in bot_requests_daily."""
    db_path, backend = db_with_raw_data

    # Insert processed user_request data
    base_time = datetime(2025, 1, 15, 10, 0, 0)
    processed_records = []

    # Cluster 1: OpenAI ChatGPT-User (user_request)
    for i in range(5):
        record = {
            "request_timestamp": (
                base_time + timedelta(milliseconds=i * 20)
            ).isoformat()
            + "Z",
            "request_date": "2025-01-15",
            "request_hour": 10,
            "day_of_week": "Wednesday",
            "request_uri": f"/mortgage/page-{i}",
            "request_host": "example.com",
            "url_path": f"/mortgage/page-{i}",
            "url_path_depth": 2,
            "user_agent_raw": "Mozilla/5.0 ChatGPT-User",
            "bot_name": "ChatGPT-User",
            "bot_provider": "OpenAI",
            "bot_category": "user_request",
            "bot_score": 1,
            "is_verified_bot": 1,
            "crawler_country": "US",
            "response_status": 200,
            "response_status_category": "success",
            "_processed_at": datetime.now().isoformat(),
        }
        processed_records.append(record)

    # Cluster 2: Perplexity (user_request)
    for i in range(3):
        record = {
            "request_timestamp": (
                base_time + timedelta(seconds=5, milliseconds=i * 30)
            ).isoformat()
            + "Z",
            "request_date": "2025-01-15",
            "request_hour": 10,
            "day_of_week": "Wednesday",
            "request_uri": f"/insurance/policy-{i}",
            "request_host": "example.com",
            "url_path": f"/insurance/policy-{i}",
            "url_path_depth": 2,
            "user_agent_raw": "Mozilla/5.0 PerplexityBot",
            "bot_name": "PerplexityBot",
            "bot_provider": "Perplexity",
            "bot_category": "user_request",
            "bot_score": 1,
            "is_verified_bot": 1,
            "crawler_country": "NL",
            "response_status": 200,
            "response_status_category": "success",
            "_processed_at": datetime.now().isoformat(),
        }
        processed_records.append(record)

    # Insert processed records
    for record in processed_records:
        columns = ", ".join(record.keys())
        placeholders = ", ".join(f":{k}" for k in record.keys())
        sql = f"INSERT INTO bot_requests_daily ({columns}) VALUES ({placeholders})"
        backend.execute(sql, record)

    yield db_path, backend


class TestRunSessionAggregation:
    """Tests for the run_session_aggregation function."""

    def test_session_aggregation_creates_sessions(self, db_with_processed_data):
        """Session aggregation should create sessions from processed data."""
        db_path, backend = db_with_processed_data

        from run_pipeline import run_session_aggregation

        result = run_session_aggregation(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
            dry_run=False,
        )

        assert result["success"] is True
        assert result["requests_processed"] == 8  # 5 + 3 user_request records
        assert result["sessions_created"] == 2  # Two separate clusters
        assert (
            result["high_confidence"]
            + result["medium_confidence"]
            + result["low_confidence"]
            == 2
        )

    def test_session_aggregation_uses_100ms_window(self, db_with_processed_data):
        """Session aggregation should use the production 100ms window."""
        db_path, backend = db_with_processed_data

        from run_pipeline import run_session_aggregation

        result = run_session_aggregation(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
        )

        assert result["success"] is True

        # Verify sessions were created with 100ms window
        sessions = backend.query("SELECT window_ms FROM query_fanout_sessions")
        assert all(s["window_ms"] == OPTIMAL_WINDOW_MS for s in sessions)

    def test_session_aggregation_dry_run(self, db_with_processed_data):
        """Dry run should not create sessions."""
        db_path, backend = db_with_processed_data

        from run_pipeline import run_session_aggregation

        result = run_session_aggregation(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
            dry_run=True,
        )

        assert result["success"] is True
        assert result["sessions_created"] == 0

        # Verify no sessions created
        sessions = backend.query("SELECT COUNT(*) as count FROM query_fanout_sessions")
        assert sessions[0]["count"] == 0

    def test_session_aggregation_empty_data(self, db_with_processed_data):
        """Session aggregation should handle empty date range gracefully."""
        db_path, backend = db_with_processed_data

        from run_pipeline import run_session_aggregation

        # Query a date range with no data
        result = run_session_aggregation(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2030, 1, 1),
            end_date=date(2030, 1, 31),
        )

        assert result["success"] is True
        assert result["requests_processed"] == 0
        assert result["sessions_created"] == 0

    def test_session_aggregation_filters_user_request_only(
        self, db_with_processed_data
    ):
        """Session aggregation should only process user_request traffic."""
        db_path, backend = db_with_processed_data

        # Add training traffic (should not be included in sessions)
        training_record = {
            "request_timestamp": "2025-01-15T10:00:10Z",
            "request_date": "2025-01-15",
            "request_hour": 10,
            "day_of_week": "Wednesday",
            "request_uri": "/training/data",
            "request_host": "example.com",
            "url_path": "/training/data",
            "url_path_depth": 2,
            "user_agent_raw": "GPTBot",
            "bot_name": "GPTBot",
            "bot_provider": "OpenAI",
            "bot_category": "training",  # Training, not user_request
            "bot_score": 1,
            "is_verified_bot": 1,
            "crawler_country": "US",
            "response_status": 200,
            "response_status_category": "success",
            "_processed_at": datetime.now().isoformat(),
        }
        columns = ", ".join(training_record.keys())
        placeholders = ", ".join(f":{k}" for k in training_record.keys())
        sql = f"INSERT INTO bot_requests_daily ({columns}) VALUES ({placeholders})"
        backend.execute(sql, training_record)

        from run_pipeline import run_session_aggregation

        result = run_session_aggregation(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
        )

        # Should still only have 8 user_request records (training excluded)
        assert result["requests_processed"] == 8
        assert result["sessions_created"] == 2


class TestPipelineCLIIntegration:
    """Tests for CLI integration with session aggregation."""

    def test_cli_skip_sessions_flag(self, db_with_processed_data):
        """CLI should support --skip-sessions flag."""
        db_path, _ = db_with_processed_data

        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_pipeline.py",
                "--backend",
                "sqlite",
                "--db-path",
                str(db_path),
                "--start-date",
                "2025-01-15",
                "--end-date",
                "2025-01-15",
                "--skip-sessions",
                "--dry-run",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        assert "Session aggregation skipped" in result.stdout

    def test_cli_shows_session_metrics(self, db_with_processed_data, tmp_path):
        """CLI should show session creation metrics."""
        db_path, _ = db_with_processed_data

        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_pipeline.py",
                "--backend",
                "sqlite",
                "--db-path",
                str(db_path),
                "--start-date",
                "2025-01-15",
                "--end-date",
                "2025-01-15",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        # Should show session aggregation section
        assert "Session Aggregation" in result.stdout
        assert "Sessions created:" in result.stdout
        assert "Confidence:" in result.stdout


class TestSessionReprocessing:
    """Tests for session reprocessing on pipeline re-run."""

    def test_rerun_deletes_and_recreates_sessions(self, db_with_processed_data):
        """Re-running pipeline should delete and recreate sessions."""
        db_path, backend = db_with_processed_data

        from run_pipeline import run_session_aggregation

        # First run
        result1 = run_session_aggregation(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
        )

        assert result1["success"] is True
        initial_count = result1["sessions_created"]

        # Get session IDs from first run
        sessions1 = backend.query(
            "SELECT session_id FROM query_fanout_sessions ORDER BY session_id"
        )
        session_ids_1 = [s["session_id"] for s in sessions1]

        # Second run (should delete and recreate)
        result2 = run_session_aggregation(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
        )

        assert result2["success"] is True
        assert result2["sessions_created"] == initial_count

        # Get session IDs from second run
        sessions2 = backend.query(
            "SELECT session_id FROM query_fanout_sessions ORDER BY session_id"
        )
        session_ids_2 = [s["session_id"] for s in sessions2]

        # Session IDs should be different (new UUIDs)
        assert session_ids_1 != session_ids_2
        assert len(sessions2) == len(sessions1)
