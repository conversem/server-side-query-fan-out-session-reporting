"""
Integration tests for SessionAggregator service.

Tests the complete session creation workflow including:
- Temporal bundling with 100ms window
- Semantic similarity calculation
- Confidence level assignment
- Session name derivation
- Database persistence
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from llm_bot_pipeline.config.constants import OPTIMAL_WINDOW_MS
from llm_bot_pipeline.reporting import (
    SessionAggregationResult,
    SessionAggregator,
    SessionRecord,
)


class TestSessionAggregatorBasic:
    """Basic tests for SessionAggregator initialization and lifecycle."""

    def test_initialization(self):
        """Should initialize with default settings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            aggregator = SessionAggregator(db_path=db_path)

            assert aggregator._backend is not None
            assert aggregator._embedder is not None
            aggregator.close()

    def test_context_manager(self):
        """Should work as context manager."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            with SessionAggregator(db_path=db_path) as aggregator:
                assert aggregator._initialized


class TestSessionCreationFromDataFrame:
    """Tests for create_sessions_from_dataframe method."""

    def _create_test_df(
        self,
        base_time: datetime,
        provider: str = "OpenAI",
        num_clusters: int = 2,
        requests_per_cluster: int = 4,
        gap_ms: int = 500,
        intra_gap_ms: int = 30,
    ) -> pd.DataFrame:
        """Create test DataFrame with known temporal patterns."""
        timestamps = []
        urls = []
        providers = []

        # Use more diverse URL patterns to avoid TF-IDF min_df/max_df issues
        url_templates = [
            "https://example.com/{topic}/home-buying-guide",
            "https://example.com/{topic}/mortgage-calculator",
            "https://example.com/{topic}/property-search",
            "https://example.com/{topic}/real-estate-tips",
            "https://example.com/{topic}/first-time-buyers",
            "https://example.com/{topic}/investment-advice",
        ]

        for cluster in range(num_clusters):
            cluster_start = base_time + timedelta(
                milliseconds=cluster * (gap_ms + intra_gap_ms * requests_per_cluster)
            )
            topic = f"topic-{cluster}"
            for req in range(requests_per_cluster):
                timestamps.append(
                    cluster_start + timedelta(milliseconds=req * intra_gap_ms)
                )
                urls.append(url_templates[req % len(url_templates)].format(topic=topic))
                providers.append(provider)

        return pd.DataFrame(
            {
                "datetime": timestamps,
                "url": urls,
                "bot_provider": providers,
            }
        )

    def test_creates_sessions_from_clustered_requests(self):
        """Should create sessions from temporally-clustered requests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                base_time = datetime(2024, 1, 15, 10, 0, 0)
                df = self._create_test_df(
                    base_time,
                    num_clusters=2,
                    requests_per_cluster=3,
                    gap_ms=500,
                    intra_gap_ms=30,
                )

                result = aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )

                assert result.success is True
                assert result.sessions_created == 2
                assert result.total_requests_bundled == 6
                assert result.mean_session_size == 3.0

    def test_uses_100ms_window(self):
        """Should use 100ms window to group requests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                base_time = datetime(2024, 1, 15, 10, 0, 0)
                # Create requests within 100ms window
                df = pd.DataFrame(
                    {
                        "datetime": [
                            base_time,
                            base_time + timedelta(milliseconds=50),
                            base_time + timedelta(milliseconds=100),
                        ],
                        "url": [
                            "https://example.com/page1",
                            "https://example.com/page2",
                            "https://example.com/page3",
                        ],
                        "bot_provider": ["OpenAI", "OpenAI", "OpenAI"],
                    }
                )

                result = aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )

                # All within 100ms should be one session
                assert result.sessions_created == 1
                assert result.total_requests_bundled == 3

    def test_calculates_confidence_levels(self):
        """Should calculate and assign confidence levels."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                base_time = datetime(2024, 1, 15, 10, 0, 0)
                df = self._create_test_df(base_time, num_clusters=3)

                result = aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )

                total_confidence = (
                    result.high_confidence_count
                    + result.medium_confidence_count
                    + result.low_confidence_count
                )
                assert total_confidence == result.sessions_created

    def test_empty_dataframe_returns_zero_sessions(self):
        """Should handle empty DataFrame gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                df = pd.DataFrame(columns=["datetime", "url", "bot_provider"])

                result = aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )

                assert result.success is True
                assert result.sessions_created == 0

    def test_groups_by_provider(self):
        """Should create separate sessions for different providers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                base_time = datetime(2024, 1, 15, 10, 0, 0)
                # Create interleaved requests from two providers
                df = pd.DataFrame(
                    {
                        "datetime": [
                            base_time,
                            base_time + timedelta(milliseconds=10),
                            base_time + timedelta(milliseconds=20),
                            base_time + timedelta(milliseconds=30),
                        ],
                        "url": [
                            "https://example.com/page1",
                            "https://example.com/page2",
                            "https://example.com/page3",
                            "https://example.com/page4",
                        ],
                        "bot_provider": [
                            "OpenAI",
                            "Perplexity",
                            "OpenAI",
                            "Perplexity",
                        ],
                    }
                )

                result = aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )

                # Should have 2 sessions (one per provider)
                assert result.sessions_created == 2


class TestSessionNameDerivation:
    """Tests for fanout_session_name derivation."""

    def test_derives_session_name_from_first_url(self):
        """Should derive session name from first URL in session."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                base_time = datetime(2024, 1, 15, 10, 0, 0)
                df = pd.DataFrame(
                    {
                        "datetime": [
                            base_time,
                            base_time + timedelta(milliseconds=50),
                        ],
                        "url": [
                            "https://example.com/blog/home-buying-guide",
                            "https://example.com/blog/mortgage-tips",
                        ],
                        "bot_provider": ["OpenAI", "OpenAI"],
                    }
                )

                aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )

                # Query the session to verify session name
                sessions = aggregator.get_sessions_by_provider("OpenAI")
                assert len(sessions) == 1
                assert sessions[0]["fanout_session_name"] == "home buying guide"


class TestDatabasePersistence:
    """Tests for database persistence functionality."""

    def test_persists_sessions_to_database(self):
        """Should persist sessions to database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                base_time = datetime(2024, 1, 15, 10, 0, 0)
                # Use multiple diverse URLs to avoid TF-IDF min_df issues
                df = pd.DataFrame(
                    {
                        "datetime": [
                            base_time,
                            base_time + timedelta(milliseconds=30),
                            base_time + timedelta(milliseconds=60),
                            base_time + timedelta(milliseconds=90),
                        ],
                        "url": [
                            "https://example.com/blog/home-buying-guide",
                            "https://example.com/blog/mortgage-tips",
                            "https://example.com/blog/property-search",
                            "https://example.com/blog/real-estate",
                        ],
                        "bot_provider": ["OpenAI", "OpenAI", "OpenAI", "OpenAI"],
                    }
                )

                result = aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )
                assert result.success is True

                summary = aggregator.get_session_summary()
                assert summary["total_sessions"] == 1

    def test_delete_sessions(self):
        """Should delete sessions based on criteria."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                base_time = datetime(2024, 1, 15, 10, 0, 0)
                df = pd.DataFrame(
                    {
                        "datetime": [
                            base_time,
                            base_time + timedelta(milliseconds=500),
                        ],
                        "url": [
                            "https://example.com/page1",
                            "https://example.com/page2",
                        ],
                        "bot_provider": ["OpenAI", "OpenAI"],
                    }
                )

                aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )
                deleted = aggregator.delete_sessions(bot_provider="OpenAI")

                assert deleted >= 1
                summary = aggregator.get_session_summary()
                assert summary["total_sessions"] == 0

    def test_get_session_summary(self):
        """Should return correct summary statistics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                base_time = datetime(2024, 1, 15, 10, 0, 0)
                df = pd.DataFrame(
                    {
                        "datetime": [
                            base_time,
                            base_time + timedelta(milliseconds=30),
                            base_time + timedelta(milliseconds=60),
                        ],
                        "url": [
                            "https://example.com/topic-a/page1",
                            "https://example.com/topic-a/page2",
                            "https://example.com/topic-a/page3",
                        ],
                        "bot_provider": ["OpenAI", "OpenAI", "OpenAI"],
                    }
                )

                aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )
                summary = aggregator.get_session_summary()

                assert summary["total_sessions"] == 1
                assert summary["total_requests"] == 3
                assert summary["avg_session_size"] == 3.0
                assert "confidence_distribution" in summary


class TestErrorHandling:
    """Tests for error handling."""

    def test_handles_invalid_data_gracefully(self):
        """Should handle errors and return failure result."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with SessionAggregator(db_path=db_path) as aggregator:
                # Missing required columns
                df = pd.DataFrame({"invalid_col": [1, 2, 3]})

                result = aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )

                assert result.success is False
                assert result.error is not None


class TestSessionRecord:
    """Tests for SessionRecord dataclass."""

    def test_session_record_has_all_fields(self):
        """SessionRecord should have all required fields."""
        record = SessionRecord(
            session_id="test-id",
            session_date="2024-01-15",
            session_start_time="2024-01-15T10:00:00",
            session_end_time="2024-01-15T10:00:01",
            duration_ms=1000.0,
            bot_provider="OpenAI",
            bot_name="ChatGPT-User",
            request_count=5,
            unique_urls=3,
            mean_cosine_similarity=0.8,
            min_cosine_similarity=0.6,
            max_cosine_similarity=0.95,
            confidence_level="high",
            fanout_session_name="home buying guide",
            url_list='["url1", "url2"]',
            window_ms=100.0,
            _created_at="2024-01-15T10:00:00Z",
        )

        assert record.session_id == "test-id"
        assert record.fanout_session_name == "home buying guide"
        assert record.confidence_level == "high"
