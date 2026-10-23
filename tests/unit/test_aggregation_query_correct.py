"""Unit tests for aggregation query generation (SQL correctness)."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.reporting.aggregations import ReportingAggregator


class TestAggregationQueryCorrect:
    """Assert generated SQL is syntactically correct."""

    @pytest.fixture
    def aggregator(self):
        """Create ReportingAggregator with mocked BigQuery client."""
        with patch(
            "llm_bot_pipeline.reporting.aggregations.bigquery.Client",
            return_value=MagicMock(),
        ):
            agg = ReportingAggregator(
                project_id="test-project",
                credentials_path=Path("/tmp/fake.json"),
            )
            yield agg

    def test_daily_summary_query_has_required_clauses(self, aggregator):
        """Generated daily_summary query contains SELECT, FROM, WHERE, GROUP BY."""
        start = date(2024, 1, 1)
        end = date(2024, 1, 7)
        query = aggregator._build_daily_summary_query(start, end)

        assert "SELECT" in query.upper()
        assert "FROM" in query.upper()
        assert "WHERE" in query.upper()
        assert "GROUP BY" in query.upper()

    def test_daily_summary_query_date_range_in_where(self, aggregator):
        """Date range appears in WHERE clause."""
        start = date(2024, 1, 1)
        end = date(2024, 1, 7)
        query = aggregator._build_daily_summary_query(start, end)

        assert "2024-01-01" in query
        assert "2024-01-07" in query
        assert "request_date" in query

    def test_url_performance_query_has_required_clauses(self, aggregator):
        """Generated url_performance query contains SELECT, FROM, WHERE, GROUP BY."""
        start = date(2024, 1, 1)
        end = date(2024, 1, 7)
        query = aggregator._build_url_performance_query(start, end)

        assert "SELECT" in query.upper()
        assert "FROM" in query.upper()
        assert "WHERE" in query.upper()
        assert "GROUP BY" in query.upper()

    def test_url_performance_query_date_range_in_where(self, aggregator):
        """Date range appears in WHERE clause."""
        start = date(2024, 1, 1)
        end = date(2024, 1, 7)
        query = aggregator._build_url_performance_query(start, end)

        assert "2024-01-01" in query
        assert "2024-01-07" in query
        assert "request_date" in query

    def _capture_queries(self, aggregator):
        calls: list[str] = []

        def _fake_query(*args, **kwargs):
            calls.append(str(args[0]))
            job = MagicMock()
            job.num_dml_affected_rows = 0
            job.result.return_value.total_rows = 0
            return job

        aggregator.client.query = MagicMock(side_effect=_fake_query)
        return calls

    def test_daily_summary_deletes_range_before_append(self, aggregator):
        """Idempotency: re-runs must clear the date range first (no dup rows)."""
        calls = self._capture_queries(aggregator)
        aggregator.aggregate_daily_summary(date(2024, 1, 1), date(2024, 1, 7))
        assert calls[0].strip().upper().startswith("DELETE")
        assert "2024-01-01" in calls[0] and "2024-01-07" in calls[0]

    def test_url_performance_deletes_range_before_append(self, aggregator):
        calls = self._capture_queries(aggregator)
        aggregator.aggregate_url_performance(date(2024, 1, 1), date(2024, 1, 7))
        assert calls[0].strip().upper().startswith("DELETE")
        assert "2024-01-01" in calls[0] and "2024-01-07" in calls[0]

    def test_delete_can_be_disabled(self, aggregator):
        calls = self._capture_queries(aggregator)
        aggregator.aggregate_daily_summary(
            date(2024, 1, 1), date(2024, 1, 7), delete_existing=False
        )
        aggregator.aggregate_url_performance(
            date(2024, 1, 1), date(2024, 1, 7), delete_existing=False
        )
        assert not any(c.strip().upper().startswith("DELETE") for c in calls)

    def test_queries_parse_as_valid_sql_structure(self, aggregator):
        """Generated queries have balanced structure (no obvious syntax errors)."""
        start = date(2024, 1, 1)
        end = date(2024, 1, 7)

        daily_query = aggregator._build_daily_summary_query(start, end)
        url_query = aggregator._build_url_performance_query(start, end)

        for query in (daily_query, url_query):
            assert query.strip()
            assert query.count("(") == query.count(")")
            assert "request_date" in query
