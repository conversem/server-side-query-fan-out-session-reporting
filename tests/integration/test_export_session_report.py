"""
Integration tests for export_session_report.py script.

Tests CSV and Excel export functionality with various filter combinations.
"""

import json
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

# Add src and scripts to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from llm_bot_pipeline.storage import get_backend


@pytest.fixture
def db_with_sessions(tmp_path):
    """Create a temporary database with test session data."""
    db_path = tmp_path / "test_export.db"
    backend = get_backend(backend_type="sqlite", db_path=db_path)
    backend.initialize()

    # Create the query_fanout_sessions table
    create_sql = """
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
    backend.execute(create_sql)

    # Insert test data with various providers and confidence levels
    test_sessions = [
        {
            "session_id": "sess-001",
            "session_date": "2025-01-15",
            "session_start_time": "2025-01-15T10:00:00Z",
            "session_end_time": "2025-01-15T10:00:05Z",
            "duration_ms": 5000,
            "bot_provider": "OpenAI",
            "bot_name": "ChatGPT-User",
            "request_count": 5,
            "unique_urls": 4,
            "mean_cosine_similarity": 0.85,
            "min_cosine_similarity": 0.72,
            "max_cosine_similarity": 0.95,
            "confidence_level": "high",
            "fanout_session_name": "mortgage calculator",
            "url_list": json.dumps(
                ["/mortgage/calc", "/mortgage/rates", "/mortgage/faq", "/about"]
            ),
            "window_ms": 100.0,
        },
        {
            "session_id": "sess-002",
            "session_date": "2025-01-15",
            "session_start_time": "2025-01-15T14:30:00Z",
            "session_end_time": "2025-01-15T14:30:02Z",
            "duration_ms": 2000,
            "bot_provider": "Perplexity",
            "bot_name": "PerplexityBot",
            "request_count": 3,
            "unique_urls": 3,
            "mean_cosine_similarity": 0.65,
            "min_cosine_similarity": 0.45,
            "max_cosine_similarity": 0.78,
            "confidence_level": "medium",
            "fanout_session_name": "first time buyer",
            "url_list": json.dumps(["/tips/buyer", "/tips/checklist", "/blog/buying"]),
            "window_ms": 100.0,
        },
        {
            "session_id": "sess-003",
            "session_date": "2025-01-16",
            "session_start_time": "2025-01-16T09:15:00Z",
            "session_end_time": "2025-01-16T09:15:01Z",
            "duration_ms": 1000,
            "bot_provider": "OpenAI",
            "bot_name": "ChatGPT-User",
            "request_count": 2,
            "unique_urls": 2,
            "mean_cosine_similarity": 0.35,
            "min_cosine_similarity": 0.25,
            "max_cosine_similarity": 0.45,
            "confidence_level": "low",
            "fanout_session_name": "contact page",
            "url_list": json.dumps(["/contact", "/locations"]),
            "window_ms": 100.0,
        },
        {
            "session_id": "sess-004",
            "session_date": "2025-01-16",
            "session_start_time": "2025-01-16T16:45:00Z",
            "session_end_time": "2025-01-16T16:45:08Z",
            "duration_ms": 8000,
            "bot_provider": "Perplexity",
            "bot_name": "PerplexityBot",
            "request_count": 7,
            "unique_urls": 6,
            "mean_cosine_similarity": 0.78,
            "min_cosine_similarity": 0.55,
            "max_cosine_similarity": 0.92,
            "confidence_level": "high",
            "fanout_session_name": "home insurance",
            "url_list": json.dumps(
                [
                    "/insurance/home",
                    "/insurance/quotes",
                    "/insurance/faq",
                    "/insurance/claims",
                    "/about",
                    "/contact",
                ]
            ),
            "window_ms": 100.0,
        },
    ]

    for session in test_sessions:
        columns = ", ".join(session.keys())
        placeholders = ", ".join(f":{k}" for k in session.keys())
        insert_sql = (
            f"INSERT INTO query_fanout_sessions ({columns}) VALUES ({placeholders})"
        )
        backend.execute(insert_sql, session)

    yield db_path, backend

    backend.close()


class TestCSVExport:
    """Tests for CSV export functionality."""

    def test_export_all_sessions_to_csv(self, db_with_sessions, tmp_path):
        """Test exporting all sessions to CSV."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "output" / "sessions.csv"

        # Import and run export function
        from export_session_report import export_to_csv

        count = export_to_csv(backend, output_path)

        assert count == 4
        assert output_path.exists()

        df = pd.read_csv(output_path)
        assert len(df) == 4
        assert "session_id" in df.columns
        assert "fanout_session_name" in df.columns
        assert "confidence_level" in df.columns

    def test_export_with_date_filter(self, db_with_sessions, tmp_path):
        """Test exporting with date range filter."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "filtered.csv"

        from export_session_report import export_to_csv

        count = export_to_csv(
            backend,
            output_path,
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
        )

        assert count == 2
        df = pd.read_csv(output_path)
        assert all(df["session_date"] == "2025-01-15")

    def test_export_with_provider_filter(self, db_with_sessions, tmp_path):
        """Test exporting with provider filter."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "openai_sessions.csv"

        from export_session_report import export_to_csv

        count = export_to_csv(backend, output_path, provider="OpenAI")

        assert count == 2
        df = pd.read_csv(output_path)
        assert all(df["bot_provider"] == "OpenAI")

    def test_export_with_min_confidence_high(self, db_with_sessions, tmp_path):
        """Test exporting with high confidence filter."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "high_conf.csv"

        from export_session_report import export_to_csv

        count = export_to_csv(backend, output_path, min_confidence="high")

        assert count == 2
        df = pd.read_csv(output_path)
        assert all(df["confidence_level"] == "high")

    def test_export_with_min_confidence_medium(self, db_with_sessions, tmp_path):
        """Test exporting with medium confidence filter includes high and medium."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "med_conf.csv"

        from export_session_report import export_to_csv

        count = export_to_csv(backend, output_path, min_confidence="medium")

        assert count == 3  # 2 high + 1 medium
        df = pd.read_csv(output_path)
        assert all(df["confidence_level"].isin(["high", "medium"]))

    def test_export_with_combined_filters(self, db_with_sessions, tmp_path):
        """Test exporting with multiple filters combined."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "combined.csv"

        from export_session_report import export_to_csv

        count = export_to_csv(
            backend,
            output_path,
            start_date=date(2025, 1, 16),
            end_date=date(2025, 1, 16),
            provider="Perplexity",
        )

        assert count == 1
        df = pd.read_csv(output_path)
        assert df.iloc[0]["session_id"] == "sess-004"

    def test_export_empty_result(self, db_with_sessions, tmp_path):
        """Test exporting when no sessions match filters."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "empty.csv"

        from export_session_report import export_to_csv

        count = export_to_csv(
            backend,
            output_path,
            start_date=date(2030, 1, 1),
            end_date=date(2030, 12, 31),
        )

        assert count == 0
        assert not output_path.exists()


class TestExcelExport:
    """Tests for Excel export functionality."""

    def test_export_all_sessions_to_excel(self, db_with_sessions, tmp_path):
        """Test exporting all sessions to Excel with multiple sheets."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "sessions.xlsx"

        from export_session_report import export_to_excel

        count = export_to_excel(backend, output_path)

        assert count == 4
        assert output_path.exists()

        # Verify all sheets exist
        sheets = pd.read_excel(output_path, sheet_name=None)
        assert "Sessions" in sheets
        assert "Summary" in sheets
        assert "Top URLs" in sheets
        assert "Provider Stats" in sheets

    def test_sessions_sheet_content(self, db_with_sessions, tmp_path):
        """Test that Sessions sheet has all required columns."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "sessions.xlsx"

        from export_session_report import export_to_excel

        export_to_excel(backend, output_path)

        df = pd.read_excel(output_path, sheet_name="Sessions")

        # Verify PRD-specified columns
        required_columns = [
            "session_id",
            "fanout_session_name",
            "session_date",
            "session_start_time",
            "session_end_time",
            "duration_ms",
            "bot_provider",
            "request_count",
            "unique_urls",
            "mean_cosine_similarity",
            "min_cosine_similarity",
            "confidence_level",
            "url_list",
        ]
        for col in required_columns:
            assert col in df.columns, f"Missing column: {col}"

    def test_summary_sheet_content(self, db_with_sessions, tmp_path):
        """Test that Summary sheet has daily aggregates."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "sessions.xlsx"

        from export_session_report import export_to_excel

        export_to_excel(backend, output_path)

        df = pd.read_excel(output_path, sheet_name="Summary")

        assert len(df) == 2  # Two days of data
        assert "session_date" in df.columns
        assert "total_sessions" in df.columns
        assert "avg_requests_per_session" in df.columns
        assert "high_confidence" in df.columns

    def test_top_urls_sheet_content(self, db_with_sessions, tmp_path):
        """Test that Top URLs sheet shows URL frequencies."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "sessions.xlsx"

        from export_session_report import export_to_excel

        export_to_excel(backend, output_path)

        df = pd.read_excel(output_path, sheet_name="Top URLs")

        assert "url" in df.columns
        assert "frequency" in df.columns
        assert "session_count" in df.columns
        # URLs should be sorted by frequency descending
        assert df["frequency"].is_monotonic_decreasing or len(df) <= 1

    def test_provider_stats_sheet_content(self, db_with_sessions, tmp_path):
        """Test that Provider Stats sheet shows breakdown by provider."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "sessions.xlsx"

        from export_session_report import export_to_excel

        export_to_excel(backend, output_path)

        df = pd.read_excel(output_path, sheet_name="Provider Stats")

        assert len(df) == 2  # OpenAI and Perplexity
        assert "bot_provider" in df.columns
        assert "total_sessions" in df.columns
        assert "high_confidence_pct" in df.columns

        # Each provider should have 2 sessions
        for _, row in df.iterrows():
            assert row["total_sessions"] == 2

    def test_excel_export_with_filters(self, db_with_sessions, tmp_path):
        """Test Excel export respects filters."""
        db_path, backend = db_with_sessions
        output_path = tmp_path / "filtered.xlsx"

        from export_session_report import export_to_excel

        count = export_to_excel(
            backend, output_path, provider="OpenAI", min_confidence="high"
        )

        assert count == 1  # Only sess-001 matches

        df = pd.read_excel(output_path, sheet_name="Sessions")
        assert len(df) == 1
        assert df.iloc[0]["session_id"] == "sess-001"


class TestCLIInterface:
    """Tests for CLI interface."""

    def test_cli_help(self):
        """Test CLI shows help correctly."""
        result = subprocess.run(
            [sys.executable, "scripts/export_session_report.py", "--help"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )
        assert result.returncode == 0
        assert "--output" in result.stdout
        assert "--format" in result.stdout
        assert "--provider" in result.stdout
        assert "--min-confidence" in result.stdout

    def test_cli_requires_output(self):
        """Test CLI requires --output argument."""
        result = subprocess.run(
            [sys.executable, "scripts/export_session_report.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )
        assert result.returncode != 0
        assert "required" in result.stderr.lower() or "output" in result.stderr.lower()

    def test_cli_csv_export(self, db_with_sessions, tmp_path):
        """Test CLI CSV export end-to-end."""
        db_path, _ = db_with_sessions
        output_path = tmp_path / "cli_test.csv"

        result = subprocess.run(
            [
                sys.executable,
                "scripts/export_session_report.py",
                "--output",
                str(output_path),
                "--db-path",
                str(db_path),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        assert output_path.exists()

        df = pd.read_csv(output_path)
        assert len(df) == 4

    def test_cli_xlsx_export(self, db_with_sessions, tmp_path):
        """Test CLI Excel export end-to-end."""
        db_path, _ = db_with_sessions
        output_path = tmp_path / "cli_test.xlsx"

        result = subprocess.run(
            [
                sys.executable,
                "scripts/export_session_report.py",
                "--format",
                "xlsx",
                "--output",
                str(output_path),
                "--db-path",
                str(db_path),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        assert output_path.exists()

        # Verify Excel has multiple sheets
        sheets = pd.read_excel(output_path, sheet_name=None)
        assert len(sheets) >= 4

    def test_cli_with_all_filters(self, db_with_sessions, tmp_path):
        """Test CLI with all filter options."""
        db_path, _ = db_with_sessions
        output_path = tmp_path / "filtered_cli.csv"

        result = subprocess.run(
            [
                sys.executable,
                "scripts/export_session_report.py",
                "--output",
                str(output_path),
                "--db-path",
                str(db_path),
                "--start-date",
                "2025-01-15",
                "--end-date",
                "2025-01-15",
                "--provider",
                "OpenAI",
                "--min-confidence",
                "high",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        assert output_path.exists()

        df = pd.read_csv(output_path)
        assert len(df) == 1
        assert df.iloc[0]["session_id"] == "sess-001"
