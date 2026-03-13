"""Unit tests for BackfillStateManager."""

from datetime import date

import pytest

from llm_bot_pipeline.pipeline.backfill_state import BackfillStateManager


class TestBackfillStateManager:
    """Tests for BackfillStateManager class."""

    def test_record_and_get_completed_dates(self, tmp_path):
        """Verify completed dates are recorded and retrieved."""
        path = tmp_path / "backfill_state.json"
        mgr = BackfillStateManager(path)
        start = date(2026, 1, 20)
        end = date(2026, 1, 25)

        mgr.record_completed(date(2026, 1, 21), start, end)
        mgr.record_completed(date(2026, 1, 22), start, end)

        completed = mgr.get_completed_dates(start, end)
        assert date(2026, 1, 21) in completed
        assert date(2026, 1, 22) in completed
        assert date(2026, 1, 20) not in completed
        assert date(2026, 1, 25) not in completed

    def test_get_dates_to_process_returns_incomplete_only(self, tmp_path):
        """Verify get_dates_to_process skips completed dates."""
        path = tmp_path / "backfill_state.json"
        mgr = BackfillStateManager(path)
        start = date(2026, 1, 20)
        end = date(2026, 1, 23)

        mgr.record_completed(date(2026, 1, 21), start, end)

        to_process = mgr.get_dates_to_process(start, end)
        assert to_process == [
            date(2026, 1, 20),
            date(2026, 1, 22),
            date(2026, 1, 23),
        ]

    def test_clear_range_removes_state(self, tmp_path):
        """Verify clear_range removes completed state for range."""
        path = tmp_path / "backfill_state.json"
        mgr = BackfillStateManager(path)
        start = date(2026, 1, 20)
        end = date(2026, 1, 25)

        mgr.record_completed(date(2026, 1, 21), start, end)
        assert len(mgr.get_completed_dates(start, end)) == 1

        mgr.clear_range(start, end)
        assert len(mgr.get_completed_dates(start, end)) == 0

    def test_different_ranges_tracked_independently(self, tmp_path):
        """Verify different backfill ranges have separate state."""
        path = tmp_path / "backfill_state.json"
        mgr = BackfillStateManager(path)

        mgr.record_completed(
            date(2026, 1, 15),
            date(2026, 1, 10),
            date(2026, 1, 20),
        )
        mgr.record_completed(
            date(2026, 2, 5),
            date(2026, 2, 1),
            date(2026, 2, 10),
        )

        completed_a = mgr.get_completed_dates(date(2026, 1, 10), date(2026, 1, 20))
        completed_b = mgr.get_completed_dates(date(2026, 2, 1), date(2026, 2, 10))

        assert completed_a == {date(2026, 1, 15)}
        assert completed_b == {date(2026, 2, 5)}
