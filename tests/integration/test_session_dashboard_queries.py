"""
Integration tests for query fan-out session dashboard queries.

Tests the KPI and dashboard query functions for the query_fanout_sessions table.
"""

import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from llm_bot_pipeline.config.constants import OPTIMAL_WINDOW_MS
from llm_bot_pipeline.reporting import LocalDashboardQueries, SessionAggregator


class TestSessionDashboardQueries:
    """Tests for query fan-out session dashboard queries."""

    @pytest.fixture
    def db_with_sessions(self):
        """Create a temporary database with test sessions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # Create sessions using SessionAggregator
            with SessionAggregator(db_path=db_path) as aggregator:
                base_time = datetime(2024, 1, 15, 10, 0, 0)

                # Create diverse session data
                df = pd.DataFrame(
                    {
                        "datetime": [
                            # Session 1: 3 requests (OpenAI)
                            base_time,
                            base_time + timedelta(milliseconds=30),
                            base_time + timedelta(milliseconds=60),
                            # Session 2: 2 requests (OpenAI)
                            base_time + timedelta(milliseconds=500),
                            base_time + timedelta(milliseconds=530),
                            # Session 3: 4 requests (Perplexity)
                            base_time + timedelta(milliseconds=1000),
                            base_time + timedelta(milliseconds=1020),
                            base_time + timedelta(milliseconds=1050),
                            base_time + timedelta(milliseconds=1080),
                        ],
                        "url": [
                            # Session 1: home buying topic
                            "https://example.nl/blog/home-buying-guide",
                            "https://example.nl/blog/mortgage-tips",
                            "https://example.nl/blog/property-search",
                            # Session 2: calculator topic
                            "https://example.nl/tools/calculator",
                            "https://example.nl/tools/rate-compare",
                            # Session 3: investment topic
                            "https://example.nl/invest/portfolio-guide",
                            "https://example.nl/invest/stock-analysis",
                            "https://example.nl/invest/fund-comparison",
                            "https://example.nl/invest/market-trends",
                        ],
                        "bot_provider": [
                            "OpenAI",
                            "OpenAI",
                            "OpenAI",
                            "OpenAI",
                            "OpenAI",
                            "Perplexity",
                            "Perplexity",
                            "Perplexity",
                            "Perplexity",
                        ],
                    }
                )

                result = aggregator.create_sessions_from_dataframe(
                    df, window_ms=OPTIMAL_WINDOW_MS
                )
                assert result.success is True
                assert result.sessions_created == 3

            yield db_path

    def test_get_sessions_per_day(self, db_with_sessions):
        """Should return session counts per day."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_sessions_per_day(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            assert result.query_name == "sessions_per_day"
            assert result.row_count >= 1
            # Should have sessions for 2024-01-15
            dates = [row["session_date"] for row in result.rows]
            assert "2024-01-15" in dates

    def test_get_sessions_per_day_with_provider_filter(self, db_with_sessions):
        """Should filter sessions by provider."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_sessions_per_day(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
                bot_provider="OpenAI",
            )

            assert result.row_count >= 1
            # OpenAI has 2 sessions
            if result.rows:
                total = sum(row["total_sessions"] for row in result.rows)
                assert total == 2

    def test_get_avg_urls_per_session(self, db_with_sessions):
        """Should calculate average URLs per session."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_avg_urls_per_session(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            assert result.query_name == "avg_urls_per_session"
            assert result.row_count == 1
            row = result.rows[0]
            assert row["total_sessions"] == 3
            # Average URLs: (3 + 2 + 4) / 3 = 3.0
            assert row["avg_urls_per_session"] == 3.0

    def test_get_multi_url_session_rate(self, db_with_sessions):
        """Should calculate multi-URL session rate (topical authority)."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_multi_url_session_rate(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            assert result.query_name == "multi_url_session_rate"
            row = result.rows[0]
            assert row["total_sessions"] == 3
            # All 3 sessions have >= 2 URLs
            assert row["multi_url_sessions"] == 3
            assert row["multi_url_rate_pct"] == 100.0

    def test_get_fanout_ratio(self, db_with_sessions):
        """Should calculate fan-out ratio (requests / sessions)."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_fanout_ratio(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            assert result.query_name == "fanout_ratio"
            row = result.rows[0]
            assert row["total_requests"] == 9  # 3 + 2 + 4
            assert row["total_sessions"] == 3
            assert row["fanout_ratio"] == 3.0  # 9 / 3

    def test_get_high_confidence_rate(self, db_with_sessions):
        """Should calculate confidence level distribution."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_high_confidence_rate(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            assert result.query_name == "high_confidence_rate"
            row = result.rows[0]
            assert row["total_sessions"] == 3
            # All confidence levels should sum to total
            total_conf = (
                row["high_confidence"]
                + row["medium_confidence"]
                + row["low_confidence"]
            )
            assert total_conf == 3

    def test_get_daily_session_summary(self, db_with_sessions):
        """Should return daily summary with all metrics."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_daily_session_summary(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            assert result.query_name == "daily_session_summary"
            assert result.row_count >= 1
            row = result.rows[0]
            assert "total_sessions" in row
            assert "avg_requests_per_session" in row
            assert "avg_urls_per_session" in row
            assert "avg_coherence" in row
            assert "high_confidence_pct" in row

    def test_get_provider_session_comparison(self, db_with_sessions):
        """Should compare metrics across providers."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_provider_session_comparison(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            assert result.query_name == "provider_session_comparison"
            assert result.row_count == 2  # OpenAI and Perplexity

            providers = {row["bot_provider"]: row for row in result.rows}
            assert "OpenAI" in providers
            assert "Perplexity" in providers
            assert providers["OpenAI"]["sessions"] == 2
            assert providers["Perplexity"]["sessions"] == 1

    def test_get_top_session_topics(self, db_with_sessions):
        """Should return top session topics by frequency."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_top_session_topics(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
                limit=10,
            )

            assert result.query_name == "top_session_topics"
            # Should have distinct session names
            assert result.row_count >= 1
            for row in result.rows:
                assert "fanout_session_name" in row
                assert "session_count" in row

    def test_get_session_kpi_summary(self, db_with_sessions):
        """Should return all KPIs in one call."""
        with LocalDashboardQueries(db_path=db_with_sessions) as queries:
            result = queries.get_session_kpi_summary(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            assert "period" in result
            assert "kpis" in result
            kpis = result["kpis"]
            assert kpis["total_sessions"] == 3
            assert kpis["total_requests"] == 9
            assert kpis["avg_urls_per_session"] == 3.0
            assert kpis["fanout_ratio"] == 3.0
            assert "multi_url_session_rate_pct" in kpis
            assert "high_confidence_rate_pct" in kpis


class TestEmptyDatabase:
    """Tests with empty database."""

    def test_queries_handle_empty_db(self):
        """All queries should handle empty database gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "empty.db"

            # Initialize database with SessionAggregator but no data
            with SessionAggregator(db_path=db_path) as aggregator:
                pass  # Just initialize tables

            with LocalDashboardQueries(db_path=db_path) as queries:
                # All these should return empty results without error
                result = queries.get_sessions_per_day(
                    start_date=date(2024, 1, 1), end_date=date(2024, 12, 31)
                )
                assert result.row_count == 0

                result = queries.get_avg_urls_per_session(
                    start_date=date(2024, 1, 1), end_date=date(2024, 12, 31)
                )
                assert result.row_count == 1  # Returns 1 row with NULL values

                summary = queries.get_session_kpi_summary(
                    start_date=date(2024, 1, 1), end_date=date(2024, 12, 31)
                )
                assert (
                    summary["kpis"]["total_sessions"] is None
                    or summary["kpis"]["total_sessions"] == 0
                )
