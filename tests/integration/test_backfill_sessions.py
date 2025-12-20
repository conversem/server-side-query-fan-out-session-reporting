"""
Integration tests for backfill_sessions.py script.

Tests:
- Basic backfill functionality
- Resume capability (skip existing sessions)
- Force reprocessing (delete and recreate)
- Batch progress tracking
- Dry run mode
- Duplicate prevention
"""

import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

# Add src and scripts to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from llm_bot_pipeline.storage import get_backend


@pytest.fixture
def db_with_processed_data(tmp_path):
    """Create database with processed bot_requests_daily data."""
    db_path = tmp_path / "test_backfill.db"
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

    # Diverse URL patterns for TF-IDF to work correctly
    url_patterns = [
        "/mortgage/calculator-rates",
        "/mortgage/interest-comparison",
        "/mortgage/fixed-variable-options",
        "/mortgage/refinance-guide",
        "/mortgage/first-time-buyer-tips",
        "/insurance/home-coverage",
        "/insurance/policy-details",
        "/insurance/claim-process",
        "/loans/personal-application",
        "/loans/business-lending",
        "/savings/high-yield-accounts",
        "/savings/term-deposits",
    ]

    # Insert processed data for 3 days
    for day_offset in range(3):
        target_date = date(2025, 1, 15) + timedelta(days=day_offset)
        date_str = target_date.isoformat()
        base_time = datetime(
            target_date.year, target_date.month, target_date.day, 10, 0, 0
        )

        # Create a cluster of user_request traffic for each day with diverse URLs
        for i in range(8):  # More requests per day
            url = url_patterns[i % len(url_patterns)]
            record = {
                "request_timestamp": (
                    base_time + timedelta(milliseconds=i * 10)
                ).isoformat()
                + "Z",
                "request_date": date_str,
                "request_hour": 10,
                "day_of_week": "Wednesday",
                "request_uri": url,
                "request_host": "example.com",
                "url_path": url,
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
            columns = ", ".join(record.keys())
            placeholders = ", ".join(f":{k}" for k in record.keys())
            sql = f"INSERT INTO bot_requests_daily ({columns}) VALUES ({placeholders})"
            backend.execute(sql, record)

    yield db_path, backend

    backend.close()


class TestBackfillBasic:
    """Tests for basic backfill functionality."""

    def test_backfill_creates_sessions(self, db_with_processed_data):
        """Backfill should create sessions for date range."""
        db_path, backend = db_with_processed_data

        from backfill_sessions import run_backfill

        result = run_backfill(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 17),
        )

        assert result.success is True
        assert result.days_processed == 3
        assert result.total_sessions_created == 3  # One session per day
        assert result.total_requests_processed == 24  # 8 requests × 3 days

    def test_backfill_result_includes_confidence_counts(self, db_with_processed_data):
        """Backfill result should include confidence distribution."""
        db_path, backend = db_with_processed_data

        from backfill_sessions import run_backfill

        result = run_backfill(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 17),
        )

        total_confidence = (
            result.high_confidence_count
            + result.medium_confidence_count
            + result.low_confidence_count
        )
        assert total_confidence == result.total_sessions_created

    def test_backfill_skips_dates_without_data(self, db_with_processed_data):
        """Backfill should skip dates with no data."""
        db_path, backend = db_with_processed_data

        from backfill_sessions import run_backfill

        # Include dates outside the data range
        result = run_backfill(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 10),  # No data for Jan 10-14
            end_date=date(2025, 1, 17),
        )

        assert result.success is True
        assert result.days_processed == 3  # Only Jan 15-17 have data


class TestBackfillResume:
    """Tests for resume capability."""

    def test_resume_skips_existing_sessions(self, db_with_processed_data):
        """Resume mode should skip dates with existing sessions."""
        db_path, backend = db_with_processed_data

        from backfill_sessions import run_backfill

        # First run - create sessions
        result1 = run_backfill(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 16),  # Only 2 days
        )

        assert result1.success is True
        initial_sessions = result1.total_sessions_created

        # Second run with resume - should skip existing
        result2 = run_backfill(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 17),  # Extend to 3 days
            resume=True,
        )

        assert result2.success is True
        assert result2.days_skipped == 2  # Jan 15-16 skipped
        assert result2.days_processed == 1  # Only Jan 17 processed

        # Verify total sessions in DB
        sessions = backend.query("SELECT COUNT(*) as count FROM query_fanout_sessions")
        assert sessions[0]["count"] == initial_sessions + 1


class TestBackfillForce:
    """Tests for force reprocessing."""

    def test_force_recreates_sessions(self, db_with_processed_data):
        """Force mode should delete and recreate sessions."""
        db_path, backend = db_with_processed_data

        from backfill_sessions import run_backfill

        # First run
        result1 = run_backfill(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
        )

        # Get session IDs from first run
        sessions1 = backend.query(
            "SELECT session_id FROM query_fanout_sessions ORDER BY session_id"
        )
        session_ids_1 = [s["session_id"] for s in sessions1]

        # Second run with force
        result2 = run_backfill(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
            force=True,
        )

        assert result2.success is True

        # Get session IDs from second run
        sessions2 = backend.query(
            "SELECT session_id FROM query_fanout_sessions ORDER BY session_id"
        )
        session_ids_2 = [s["session_id"] for s in sessions2]

        # Session IDs should be different (new UUIDs)
        assert session_ids_1 != session_ids_2
        assert len(sessions2) == len(sessions1)


class TestBackfillDryRun:
    """Tests for dry run mode."""

    def test_dry_run_creates_no_sessions(self, db_with_processed_data):
        """Dry run should not create any sessions."""
        db_path, backend = db_with_processed_data

        from backfill_sessions import run_backfill

        result = run_backfill(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 17),
            dry_run=True,
        )

        assert result.success is True
        assert result.total_sessions_created == 0

        # Verify no sessions in DB
        sessions = backend.query("SELECT COUNT(*) as count FROM query_fanout_sessions")
        assert sessions[0]["count"] == 0


class TestBackfillCLI:
    """Tests for CLI interface."""

    def test_cli_help(self):
        """CLI should show help correctly."""
        result = subprocess.run(
            [sys.executable, "scripts/backfill_sessions.py", "--help"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )
        assert result.returncode == 0
        assert "--start-date" in result.stdout
        assert "--end-date" in result.stdout
        assert "--resume" in result.stdout
        assert "--force" in result.stdout

    def test_cli_requires_dates(self):
        """CLI should require start and end dates."""
        result = subprocess.run(
            [sys.executable, "scripts/backfill_sessions.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )
        assert result.returncode != 0
        assert "required" in result.stderr.lower()

    def test_cli_validates_date_order(self):
        """CLI should validate start_date <= end_date."""
        result = subprocess.run(
            [
                sys.executable,
                "scripts/backfill_sessions.py",
                "--start-date",
                "2025-01-31",
                "--end-date",
                "2025-01-01",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )
        assert result.returncode != 0
        assert "start-date" in result.stderr.lower()

    def test_cli_rejects_resume_and_force(self):
        """CLI should reject both --resume and --force."""
        result = subprocess.run(
            [
                sys.executable,
                "scripts/backfill_sessions.py",
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-01-31",
                "--resume",
                "--force",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )
        assert result.returncode != 0
        assert "resume" in result.stderr.lower() or "force" in result.stderr.lower()

    def test_cli_backfill_execution(self, db_with_processed_data):
        """CLI should execute backfill successfully."""
        db_path, _ = db_with_processed_data

        result = subprocess.run(
            [
                sys.executable,
                "scripts/backfill_sessions.py",
                "--start-date",
                "2025-01-15",
                "--end-date",
                "2025-01-17",
                "--db-path",
                str(db_path),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        assert "Sessions created:" in result.stdout
        assert "Days processed:" in result.stdout


class TestBackfillDataclass:
    """Tests for result dataclasses."""

    def test_backfill_result_fields(self, db_with_processed_data):
        """BackfillResult should have all required fields."""
        db_path, _ = db_with_processed_data

        from backfill_sessions import run_backfill

        result = run_backfill(
            backend_type="sqlite",
            db_path=db_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 17),
        )

        # Check all fields exist
        assert hasattr(result, "success")
        assert hasattr(result, "start_date")
        assert hasattr(result, "end_date")
        assert hasattr(result, "days_processed")
        assert hasattr(result, "days_skipped")
        assert hasattr(result, "total_sessions_created")
        assert hasattr(result, "total_requests_processed")
        assert hasattr(result, "high_confidence_count")
        assert hasattr(result, "medium_confidence_count")
        assert hasattr(result, "low_confidence_count")
        assert hasattr(result, "duration_seconds")
        assert hasattr(result, "errors")

        # Check duration is tracked
        assert result.duration_seconds > 0
