"""
Integration tests for reporting aggregations and dashboard queries.

Tests:
- Daily summary aggregation
- URL performance aggregation
- Dashboard KPI queries
- Executive summary
- Date range handling
"""

from datetime import date, timedelta

import pytest

from tests.integration.conftest import generate_sample_records


class TestAggregationOperations:
    """Tests for reporting aggregations."""

    @pytest.fixture
    def aggregator_with_clean_data(self, temp_db_path, date_range):
        """
        Create aggregator with pre-populated clean data.

        Inserts clean records directly to test aggregation logic.
        """
        from llm_bot_pipeline.reporting import LocalReportingAggregator
        from llm_bot_pipeline.storage import get_backend

        start_date, end_date = date_range
        backend = get_backend("sqlite", db_path=temp_db_path)
        backend.initialize()

        # Insert clean records for aggregation testing
        clean_records = []
        for i in range(50):
            day_offset = i % 3
            record_date = start_date + timedelta(days=day_offset)

            clean_records.append(
                {
                    "request_date": record_date.isoformat(),
                    "request_timestamp": f"{record_date}T10:00:00+00:00",
                    "request_hour": 10 + (i % 12),
                    "day_of_week": record_date.strftime("%A"),
                    "request_host": "example.com",
                    "request_uri": f"/docs/page{i % 5}",
                    "url_path": f"/docs/page{i % 5}",
                    "url_path_depth": 2,
                    "bot_name": ["GPTBot", "ChatGPT-User", "ClaudeBot"][i % 3],
                    "bot_provider": ["OpenAI", "OpenAI", "Anthropic"][i % 3],
                    "bot_category": ["training", "user_request", "training"][i % 3],
                    "bot_score": 5 + (i % 20),
                    "is_verified_bot": 1,
                    "crawler_country": ["US", "DE", "GB"][i % 3],
                    "response_status": [200, 200, 200, 304, 404][i % 5],
                    "response_status_category": [
                        "2xx_success",
                        "2xx_success",
                        "2xx_success",
                        "3xx_redirect",
                        "4xx_client_error",
                    ][i % 5],
                }
            )

        backend.insert_clean_records(clean_records)
        backend.close()

        # Create aggregator
        aggregator = LocalReportingAggregator(
            backend_type="sqlite", db_path=temp_db_path
        )
        aggregator.initialize()
        yield aggregator, start_date, end_date
        aggregator.close()

    def test_aggregate_daily_summary(self, aggregator_with_clean_data):
        """Should create daily summary aggregations."""
        aggregator, start_date, end_date = aggregator_with_clean_data

        result = aggregator.aggregate_daily_summary(start_date, end_date)

        assert result.success
        assert result.table_name == "daily_summary"
        assert result.rows_inserted > 0

    def test_aggregate_url_performance(self, aggregator_with_clean_data):
        """Should create URL performance aggregations."""
        aggregator, start_date, end_date = aggregator_with_clean_data

        result = aggregator.aggregate_url_performance(start_date, end_date)

        assert result.success
        assert result.table_name == "url_performance"
        assert result.rows_inserted > 0

    def test_aggregate_all(self, aggregator_with_clean_data):
        """Should run all aggregations."""
        aggregator, start_date, end_date = aggregator_with_clean_data

        results = aggregator.aggregate_all(start_date, end_date)

        assert len(results) == 2
        assert all(r.success for r in results)

    def test_aggregation_calculates_metrics(self, aggregator_with_clean_data):
        """Daily summary should calculate correct metrics."""
        aggregator, start_date, end_date = aggregator_with_clean_data

        aggregator.aggregate_daily_summary(start_date, end_date)

        # Query the aggregated data
        result = aggregator._backend.query(
            "SELECT total_requests, successful_requests FROM daily_summary LIMIT 1"
        )

        assert result
        assert result[0]["total_requests"] > 0
        assert result[0]["successful_requests"] >= 0


class TestDashboardQueries:
    """Tests for dashboard KPI queries."""

    @pytest.fixture
    def dashboard_with_data(self, temp_db_path, date_range):
        """
        Dashboard with pre-populated aggregated data.
        """
        from llm_bot_pipeline.reporting import (
            LocalDashboardQueries,
            LocalReportingAggregator,
        )
        from llm_bot_pipeline.storage import get_backend

        start_date, end_date = date_range
        backend = get_backend("sqlite", db_path=temp_db_path)
        backend.initialize()

        # Insert clean records
        clean_records = []
        for i in range(100):
            day_offset = i % 3
            record_date = start_date + timedelta(days=day_offset)

            clean_records.append(
                {
                    "request_date": record_date.isoformat(),
                    "request_timestamp": f"{record_date}T{10 + i % 12}:00:00+00:00",
                    "request_hour": 10 + (i % 12),
                    "day_of_week": record_date.strftime("%A"),
                    "request_host": "example.com",
                    "request_uri": f"/docs/page{i % 10}",
                    "url_path": f"/docs/page{i % 10}",
                    "url_path_depth": 2,
                    "bot_name": ["GPTBot", "ChatGPT-User", "ClaudeBot", "Claude-User"][
                        i % 4
                    ],
                    "bot_provider": ["OpenAI", "OpenAI", "Anthropic", "Anthropic"][
                        i % 4
                    ],
                    "bot_category": [
                        "training",
                        "user_request",
                        "training",
                        "user_request",
                    ][i % 4],
                    "bot_score": 5 + (i % 20),
                    "is_verified_bot": 1,
                    "crawler_country": ["US", "DE", "GB"][i % 3],
                    "response_status": 200,
                    "response_status_category": "2xx_success",
                }
            )

        backend.insert_clean_records(clean_records)
        backend.close()

        # Run aggregations
        aggregator = LocalReportingAggregator(
            backend_type="sqlite", db_path=temp_db_path
        )
        aggregator.initialize()
        aggregator.aggregate_all(start_date, end_date)
        aggregator.close()

        # Create dashboard
        dashboard = LocalDashboardQueries(backend_type="sqlite", db_path=temp_db_path)
        dashboard.initialize()
        yield dashboard, start_date, end_date
        dashboard.close()

    def test_get_executive_summary(self, dashboard_with_data):
        """Should return executive summary dict."""
        dashboard, start_date, end_date = dashboard_with_data

        summary = dashboard.get_executive_summary(days=7)

        assert "period" in summary
        assert "days" in summary
        assert "metrics" in summary

    def test_get_bot_provider_diversity(self, dashboard_with_data):
        """Should return provider diversity metrics."""
        dashboard, start_date, end_date = dashboard_with_data

        result = dashboard.get_bot_provider_diversity(start_date, end_date)

        assert result.query_name == "bot_provider_diversity"
        assert result.row_count > 0

    def test_get_response_success_rate(self, dashboard_with_data):
        """Should return success rate metrics."""
        dashboard, start_date, end_date = dashboard_with_data

        result = dashboard.get_response_success_rate(start_date, end_date)

        assert result.query_name == "response_success_rate"
        assert result.row_count > 0

    def test_get_daily_activity_trend(self, dashboard_with_data):
        """Should return daily activity trend."""
        dashboard, start_date, end_date = dashboard_with_data

        result = dashboard.get_daily_activity_trend(days=7)

        assert result.query_name == "daily_activity_trend"
        # May have 0 rows if dates don't match

    def test_get_user_request_bot_share(self, dashboard_with_data):
        """Should return bot category share."""
        dashboard, start_date, end_date = dashboard_with_data

        result = dashboard.get_user_request_bot_share(start_date, end_date)

        assert result.query_name == "user_request_bot_share"


class TestFreshnessStats:
    """Tests for data freshness statistics."""

    def test_get_freshness_stats_empty(self, local_aggregator):
        """Should return stats for empty tables."""
        stats = local_aggregator.get_freshness_stats()

        assert "daily_summary" in stats
        assert "url_performance" in stats

    def test_get_freshness_stats_with_data(self, temp_db_path, date_range):
        """Should return stats with populated data."""
        from llm_bot_pipeline.reporting import LocalReportingAggregator
        from llm_bot_pipeline.storage import get_backend

        start_date, end_date = date_range
        backend = get_backend("sqlite", db_path=temp_db_path)
        backend.initialize()

        # Insert clean records
        clean_records = [
            {
                "request_date": start_date.isoformat(),
                "request_timestamp": f"{start_date}T10:00:00+00:00",
                "request_hour": 10,
                "day_of_week": start_date.strftime("%A"),
                "request_host": "example.com",
                "request_uri": "/docs/test",
                "url_path": "/docs/test",
                "url_path_depth": 2,
                "bot_name": "GPTBot",
                "bot_provider": "OpenAI",
                "bot_category": "training",
                "bot_score": 5,
                "is_verified_bot": 1,
                "crawler_country": "US",
                "response_status": 200,
                "response_status_category": "2xx_success",
            }
        ]
        backend.insert_clean_records(clean_records)
        backend.close()

        aggregator = LocalReportingAggregator(
            backend_type="sqlite", db_path=temp_db_path
        )
        aggregator.initialize()
        aggregator.aggregate_all(start_date, end_date)

        stats = aggregator.get_freshness_stats()
        aggregator.close()

        assert stats["daily_summary"]["total_rows"] > 0
        assert stats["url_performance"]["total_rows"] > 0

