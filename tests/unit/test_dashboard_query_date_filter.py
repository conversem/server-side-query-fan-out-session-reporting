"""Unit tests for dashboard query date filtering."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.reporting.dashboard_queries import DashboardQueries


class TestDashboardQueryDateFilter:
    """Assert date range appears in WHERE clause of dashboard queries."""

    @pytest.fixture
    def dashboard(self):
        """Create DashboardQueries with mocked BigQuery client."""
        with patch(
            "llm_bot_pipeline.reporting.dashboard_queries.bigquery.Client",
            return_value=MagicMock(),
        ):
            dq = DashboardQueries(
                project_id="test-project",
                credentials_path=Path("/tmp/fake.json"),
            )
            yield dq

    def test_get_user_request_bot_share_date_in_where(self, dashboard):
        """get_user_request_bot_share includes date range in WHERE."""
        start = date(2024, 2, 1)
        end = date(2024, 2, 7)
        captured_queries = []

        def capture_query(q):
            captured_queries.append(q)
            return []

        dashboard._execute_query = capture_query
        dashboard.get_user_request_bot_share(start_date=start, end_date=end)

        assert len(captured_queries) == 1
        query = captured_queries[0]
        assert "2024-02-01" in query
        assert "2024-02-07" in query
        assert "request_date" in query
        assert "WHERE" in query.upper()

    def test_get_bot_provider_diversity_date_in_where(self, dashboard):
        """get_bot_provider_diversity includes date range in WHERE."""
        start = date(2024, 3, 1)
        end = date(2024, 3, 14)
        captured_queries = []

        def capture_query(q):
            captured_queries.append(q)
            return []

        dashboard._execute_query = capture_query
        dashboard.get_bot_provider_diversity(start_date=start, end_date=end)

        assert len(captured_queries) == 1
        query = captured_queries[0]
        assert "2024-03-01" in query
        assert "2024-03-14" in query
        assert "request_date" in query
        assert "WHERE" in query.upper()
