"""Unit tests for freshness tracker staleness detection."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from llm_bot_pipeline.reporting.freshness_tracker import DataFreshnessTracker
from llm_bot_pipeline.storage import get_backend


class TestFreshnessTrackerStaleData:
    """Feed old data, assert staleness detected."""

    def test_stale_data_detected(self, temp_db_path):
        """When last_updated_at is old, get_stale_tables returns the table."""
        backend = get_backend("sqlite", db_path=temp_db_path)
        tracker = DataFreshnessTracker(backend=backend)
        tracker.initialize()

        # Simulate update from 3 days ago
        old_time = datetime.now(timezone.utc) - timedelta(days=3)
        with patch("llm_bot_pipeline.reporting.freshness_tracker.datetime") as mock_dt:
            mock_dt.now.return_value = old_time
            mock_dt.fromisoformat = datetime.fromisoformat

            tracker.update_freshness(
                table_name="daily_summary",
                last_processed_date=date.today() - timedelta(days=3),
                rows_processed=100,
            )

        # Tables not updated within 1 day are stale
        stale = tracker.get_stale_tables(max_age_days=1)
        assert "daily_summary" in stale
        tracker.close()

    def test_fresh_data_not_stale(self, temp_db_path):
        """When last_updated_at is recent, get_stale_tables returns empty."""
        backend = get_backend("sqlite", db_path=temp_db_path)
        tracker = DataFreshnessTracker(backend=backend)
        tracker.initialize()

        # Update with current time (default)
        tracker.update_freshness(
            table_name="url_performance",
            last_processed_date=date.today(),
            rows_processed=50,
        )

        stale = tracker.get_stale_tables(max_age_days=1)
        assert "url_performance" not in stale
        tracker.close()
