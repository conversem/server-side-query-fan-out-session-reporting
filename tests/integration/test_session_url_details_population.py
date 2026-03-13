"""
Integration tests for session_url_details population functionality.

Tests the URL flattening from query_fanout_sessions to session_url_details:
- JSON parsing of url_list
- Proper url_position assignment (1-based)
- Denormalization of session metadata
- Batch processing
- Error handling for malformed JSON
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from llm_bot_pipeline.reporting import SessionAggregator
from llm_bot_pipeline.storage import get_backend


class TestSessionUrlDetailsPopulation:
    """Tests for populate_url_details method."""

    def _create_test_session(
        self,
        backend,
        session_id: str,
        urls: list[str],
        session_date: str = "2024-01-15",
        bot_provider: str = "OpenAI",
        bot_name: str = "ChatGPT-User",
    ) -> None:
        """Insert a test session directly into query_fanout_sessions."""
        sql = """
            INSERT INTO query_fanout_sessions (
                session_id, session_date, session_start_time, session_end_time,
                duration_ms, bot_provider, bot_name, request_count, unique_urls,
                mean_cosine_similarity, min_cosine_similarity, max_cosine_similarity,
                confidence_level, fanout_session_name, url_list, window_ms
            ) VALUES (
                :session_id, :session_date, :session_start_time, :session_end_time,
                :duration_ms, :bot_provider, :bot_name, :request_count, :unique_urls,
                :mean_cosine_similarity, :min_cosine_similarity, :max_cosine_similarity,
                :confidence_level, :fanout_session_name, :url_list, :window_ms
            )
        """
        params = {
            "session_id": session_id,
            "session_date": session_date,
            "session_start_time": f"{session_date}T10:00:00",
            "session_end_time": f"{session_date}T10:00:01",
            "duration_ms": 1000,
            "bot_provider": bot_provider,
            "bot_name": bot_name,
            "request_count": len(urls),
            "unique_urls": len(urls),
            "mean_cosine_similarity": 0.85,
            "min_cosine_similarity": 0.7,
            "max_cosine_similarity": 0.95,
            "confidence_level": "high",
            "fanout_session_name": "test topic",
            "url_list": json.dumps(urls),
            "window_ms": 100.0,
        }
        backend.execute(sql, params)

    def test_populates_url_details_from_session(self):
        """Should create URL detail rows for each URL in session."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            backend = get_backend("sqlite", db_path=db_path)
            backend.initialize()

            urls = [
                "https://example.com/page1",
                "https://example.com/page2",
                "https://example.com/page3",
            ]
            self._create_test_session(backend, "session-1", urls)

            with SessionAggregator(backend=backend) as aggregator:
                inserted = aggregator.populate_url_details()

            assert inserted == 3

            details = backend.query(
                "SELECT * FROM session_url_details ORDER BY url_position"
            )
            assert len(details) == 3

            backend.close()

    def test_url_position_is_one_based(self):
        """URL position should start at 1, not 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            backend = get_backend("sqlite", db_path=db_path)
            backend.initialize()

            urls = ["https://example.com/first", "https://example.com/second"]
            self._create_test_session(backend, "session-1", urls)

            with SessionAggregator(backend=backend) as aggregator:
                aggregator.populate_url_details()

            details = backend.query(
                "SELECT url, url_position FROM session_url_details ORDER BY url_position"
            )
            assert details[0]["url_position"] == 1
            assert details[0]["url"] == "https://example.com/first"
            assert details[1]["url_position"] == 2
            assert details[1]["url"] == "https://example.com/second"

            backend.close()

    def test_denormalizes_session_metadata(self):
        """Should copy session metadata to each URL detail row."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            backend = get_backend("sqlite", db_path=db_path)
            backend.initialize()

            urls = ["https://example.com/page1"]
            self._create_test_session(
                backend,
                "session-1",
                urls,
                bot_provider="Anthropic",
                bot_name="ClaudeBot",
            )

            with SessionAggregator(backend=backend) as aggregator:
                aggregator.populate_url_details()

            details = backend.query("SELECT * FROM session_url_details")
            assert len(details) == 1
            detail = details[0]

            assert detail["session_id"] == "session-1"
            assert detail["bot_provider"] == "Anthropic"
            assert detail["bot_name"] == "ClaudeBot"
            assert detail["session_unique_urls"] == 1
            assert detail["confidence_level"] == "high"
            assert detail["mean_cosine_similarity"] == 0.85

            backend.close()

    def test_filters_by_date_range(self):
        """Should only process sessions within specified date range."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            backend = get_backend("sqlite", db_path=db_path)
            backend.initialize()

            self._create_test_session(
                backend,
                "session-1",
                ["https://example.com/1"],
                session_date="2024-01-15",
            )
            self._create_test_session(
                backend,
                "session-2",
                ["https://example.com/2"],
                session_date="2024-01-16",
            )

            with SessionAggregator(backend=backend) as aggregator:
                inserted = aggregator.populate_url_details(
                    start_date="2024-01-15", end_date="2024-01-15"
                )

            assert inserted == 1

            details = backend.query("SELECT session_date FROM session_url_details")
            assert len(details) == 1
            assert details[0]["session_date"] == "2024-01-15"

            backend.close()

    def test_handles_empty_url_list(self):
        """Should handle sessions with empty url_list gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            backend = get_backend("sqlite", db_path=db_path)
            backend.initialize()

            self._create_test_session(backend, "session-1", [])

            with SessionAggregator(backend=backend) as aggregator:
                inserted = aggregator.populate_url_details()

            assert inserted == 0

            backend.close()

    def test_handles_malformed_json_gracefully(self):
        """Should log warning but continue processing other sessions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            backend = get_backend("sqlite", db_path=db_path)
            backend.initialize()

            sql = """
                INSERT INTO query_fanout_sessions (
                    session_id, session_date, session_start_time, session_end_time,
                    duration_ms, bot_provider, request_count, unique_urls,
                    confidence_level, url_list, window_ms
                ) VALUES (
                    'bad-json', '2024-01-15', '2024-01-15T10:00:00', '2024-01-15T10:00:01',
                    1000, 'OpenAI', 1, 1, 'high', 'not valid json[', 100.0
                )
            """
            backend.execute(sql, {})

            self._create_test_session(
                backend, "good-session", ["https://example.com/page1"]
            )

            with SessionAggregator(backend=backend) as aggregator:
                inserted = aggregator.populate_url_details()

            assert inserted == 1

            backend.close()

    def test_delete_url_details(self):
        """Should delete URL details with optional date filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            backend = get_backend("sqlite", db_path=db_path)
            backend.initialize()

            self._create_test_session(
                backend,
                "session-1",
                ["https://example.com/1"],
                session_date="2024-01-15",
            )
            self._create_test_session(
                backend,
                "session-2",
                ["https://example.com/2"],
                session_date="2024-01-16",
            )

            with SessionAggregator(backend=backend) as aggregator:
                aggregator.populate_url_details()

                deleted = aggregator.delete_url_details(
                    start_date="2024-01-15", end_date="2024-01-15"
                )
                assert deleted == 1

                remaining = backend.query("SELECT * FROM session_url_details")
                assert len(remaining) == 1
                assert remaining[0]["session_date"] == "2024-01-16"

            backend.close()

    def test_processes_multiple_sessions(self):
        """Should correctly process multiple sessions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            backend = get_backend("sqlite", db_path=db_path)
            backend.initialize()

            self._create_test_session(
                backend,
                "session-1",
                ["https://example.com/a1", "https://example.com/a2"],
            )
            self._create_test_session(
                backend,
                "session-2",
                [
                    "https://example.com/b1",
                    "https://example.com/b2",
                    "https://example.com/b3",
                ],
            )

            with SessionAggregator(backend=backend) as aggregator:
                inserted = aggregator.populate_url_details()

            assert inserted == 5  # 2 + 3

            backend.close()


class TestSessionUrlDetailsRowCounts:
    """Tests to verify URL expansion produces correct row counts."""

    def test_row_count_matches_url_expansion(self):
        """Total rows should equal sum of URLs across all sessions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            backend = get_backend("sqlite", db_path=db_path)
            backend.initialize()

            for i, url_count in enumerate([1, 3, 5, 2]):
                urls = [f"https://example.com/page{j}" for j in range(url_count)]
                sql = """
                    INSERT INTO query_fanout_sessions (
                        session_id, session_date, session_start_time, session_end_time,
                        duration_ms, bot_provider, request_count, unique_urls,
                        confidence_level, url_list, window_ms
                    ) VALUES (
                        :session_id, '2024-01-15', '2024-01-15T10:00:00',
                        '2024-01-15T10:00:01', 1000, 'OpenAI', :count, :count,
                        'high', :url_list, 100.0
                    )
                """
                backend.execute(
                    sql,
                    {
                        "session_id": f"session-{i}",
                        "count": url_count,
                        "url_list": json.dumps(urls),
                    },
                )

            with SessionAggregator(backend=backend) as aggregator:
                inserted = aggregator.populate_url_details()

            expected_total = 1 + 3 + 5 + 2  # 11
            assert inserted == expected_total

            backend.close()
