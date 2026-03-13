"""Tests for TemporalBundler class."""

import pandas as pd
import pytest

from llm_bot_pipeline.reporting.temporal_bundler import BundleResult, TemporalBundler


def _make_df(timestamps, urls=None, providers=None):
    """Helper to build a request DataFrame from timestamp strings."""
    n = len(timestamps)
    return pd.DataFrame(
        {
            "datetime": pd.to_datetime(timestamps),
            "url": urls or [f"https://example.com/page{i}" for i in range(n)],
            "bot_provider": providers or ["openai"] * n,
        }
    )


class TestTemporalBundlerGroupsByTime:
    """feed timestamped records and assert correct bundle grouping."""

    def test_single_bundle_within_window(self):
        df = _make_df(
            [
                "2025-01-01T00:00:00",
                "2025-01-01T00:00:01",
                "2025-01-01T00:00:02",
            ]
        )
        bundler = TemporalBundler(window_ms=5000)
        result = bundler.bundle_by_time(df)

        assert result.bundle_count == 1
        assert result.total_requests == 3

    def test_two_bundles_across_gap(self):
        df = _make_df(
            [
                "2025-01-01T00:00:00",
                "2025-01-01T00:00:01",
                "2025-01-01T00:00:10",  # 9s gap => new bundle at 5s window
            ]
        )
        bundler = TemporalBundler(window_ms=5000)
        result = bundler.bundle_by_time(df)

        assert result.bundle_count == 2
        sizes = sorted(b.request_count for b in result.bundles)
        assert sizes == [1, 2]

    def test_separate_providers_get_own_bundles(self):
        df = _make_df(
            [
                "2025-01-01T00:00:00",
                "2025-01-01T00:00:00",
            ],
            providers=["openai", "anthropic"],
        )
        bundler = TemporalBundler(window_ms=5000)
        result = bundler.bundle_by_time(df)

        assert result.bundle_count == 2
        assert sorted(result.providers_processed) == ["anthropic", "openai"]

    def test_statistics_returns_dict(self):
        df = _make_df(
            [
                "2025-01-01T00:00:00",
                "2025-01-01T00:00:01",
            ]
        )
        bundler = TemporalBundler(window_ms=5000)
        result = bundler.bundle_by_time(df)

        stats = result.statistics()
        assert stats["total_bundles"] == 1
        assert stats["total_requests"] == 2


class TestSessionTimeoutBoundary:
    """Test requests at exact session timeout boundary."""

    def test_exactly_at_boundary_stays_in_bundle(self):
        df = _make_df(
            [
                "2025-01-01T00:00:00.000",
                "2025-01-01T00:00:05.000",  # exactly 5000ms
            ]
        )
        bundler = TemporalBundler(window_ms=5000)
        result = bundler.bundle_by_time(df)

        assert result.bundle_count == 1

    def test_one_ms_over_boundary_splits(self):
        df = _make_df(
            [
                "2025-01-01T00:00:00.000",
                "2025-01-01T00:00:05.001",  # 5001ms => new bundle
            ]
        )
        bundler = TemporalBundler(window_ms=5000)
        result = bundler.bundle_by_time(df)

        assert result.bundle_count == 2

    def test_is_session_timeout_at_boundary(self):
        bundler = TemporalBundler(window_ms=5000)
        assert bundler.is_session_timeout(5001) is True
        assert bundler.is_session_timeout(5000) is False
        assert bundler.is_session_timeout(4999) is False

    def test_is_session_timeout_custom_threshold(self):
        bundler = TemporalBundler(window_ms=5000)
        assert bundler.is_session_timeout(3000, timeout_ms=2000) is True
        assert bundler.is_session_timeout(1000, timeout_ms=2000) is False


class TestEmptyInput:
    """Handle empty record set gracefully."""

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["datetime", "url", "bot_provider"])
        bundler = TemporalBundler(window_ms=5000)
        result = bundler.bundle_by_time(df)

        assert result.bundle_count == 0
        assert result.total_requests == 0
        assert result.providers_processed == []
        assert result.mean_bundle_size == 0.0

    def test_empty_statistics(self):
        df = pd.DataFrame(columns=["datetime", "url", "bot_provider"])
        bundler = TemporalBundler(window_ms=5000)
        result = bundler.bundle_by_time(df)

        stats = result.statistics()
        assert stats["total_bundles"] == 0
        assert stats["total_requests"] == 0


class TestGroupRequests:
    """Test group_requests convenience method with optional timeout override."""

    def test_override_timeout(self):
        df = _make_df(
            [
                "2025-01-01T00:00:00",
                "2025-01-01T00:00:03",  # 3s gap
            ]
        )
        bundler = TemporalBundler(window_ms=10000)

        result_wide = bundler.group_requests(df)
        assert result_wide.bundle_count == 1

        result_narrow = bundler.group_requests(df, session_timeout_ms=2000)
        assert result_narrow.bundle_count == 2

        # Instance window_ms should be restored
        assert bundler.window_ms == 10000

    def test_group_requests_none_timeout_uses_default(self):
        df = _make_df(["2025-01-01T00:00:00"])
        bundler = TemporalBundler(window_ms=5000)
        result = bundler.group_requests(df, session_timeout_ms=None)
        assert result.bundle_count == 1


class TestEstimateSessionCount:
    """Test lightweight session-count estimation."""

    def test_estimate_matches_bundle_count(self):
        df = _make_df(
            [
                "2025-01-01T00:00:00",
                "2025-01-01T00:00:01",
                "2025-01-01T00:00:10",
            ]
        )
        bundler = TemporalBundler(window_ms=5000)
        assert bundler.estimate_session_count(df) == 2

    def test_estimate_empty(self):
        df = pd.DataFrame(columns=["datetime", "url", "bot_provider"])
        bundler = TemporalBundler(window_ms=5000)
        assert bundler.estimate_session_count(df) == 0


class TestValidation:
    """Test input validation."""

    def test_negative_window_raises(self):
        with pytest.raises(ValueError, match="positive"):
            TemporalBundler(window_ms=-1)

    def test_zero_window_raises(self):
        with pytest.raises(ValueError, match="positive"):
            TemporalBundler(window_ms=0)

    def test_missing_columns_raises(self):
        df = pd.DataFrame({"wrong_col": [1]})
        bundler = TemporalBundler(window_ms=5000)
        with pytest.raises(KeyError, match="Missing required columns"):
            bundler.bundle_by_time(df)

    def test_none_dataframe_raises(self):
        bundler = TemporalBundler(window_ms=5000)
        with pytest.raises(ValueError, match="must not be None"):
            bundler.bundle_by_time(None)

    def test_negative_session_timeout_override_raises(self):
        df = _make_df(["2025-01-01T00:00:00"])
        bundler = TemporalBundler(window_ms=5000)
        with pytest.raises(ValueError, match="positive"):
            bundler.group_requests(df, session_timeout_ms=-1)
