"""
Unit tests for temporal analysis and session grouping algorithm.

Tests the core session grouping logic that uses a 100ms time window
to bundle temporally-clustered requests into sessions.
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from llm_bot_pipeline.config.constants import OPTIMAL_WINDOW_MS
from llm_bot_pipeline.research.temporal_analysis import (
    Bundle,
    TemporalAnalyzer,
    compute_bundle_statistics,
    compute_delta_stats,
    compute_inter_request_deltas,
    create_temporal_bundles,
)


class TestOptimalWindowConstant:
    """Tests for the OPTIMAL_WINDOW_MS constant."""

    def test_optimal_window_is_100ms(self):
        """OPTIMAL_WINDOW_MS should be 100ms as validated by research."""
        assert OPTIMAL_WINDOW_MS == 100


class TestCreateTemporalBundles:
    """Tests for create_temporal_bundles function."""

    def _create_requests_df(
        self, timestamps: list[datetime], provider: str = "OpenAI"
    ) -> pd.DataFrame:
        """Helper to create a DataFrame with test requests."""
        return pd.DataFrame(
            {
                "datetime": timestamps,
                "url": [f"https://example.com/page{i}" for i in range(len(timestamps))],
                "bot_provider": [provider] * len(timestamps),
            }
        )

    def test_single_request_creates_single_bundle(self):
        """A single request should create a single bundle with size 1."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        df = self._create_requests_df([base_time])

        bundles = create_temporal_bundles(df, window_ms=100)

        assert len(bundles) == 1
        assert bundles[0].request_count == 1

    def test_tight_cluster_within_100ms(self):
        """Requests within 100ms should be grouped into a single bundle."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        timestamps = [
            base_time,
            base_time + timedelta(milliseconds=20),
            base_time + timedelta(milliseconds=50),
            base_time + timedelta(milliseconds=80),
        ]
        df = self._create_requests_df(timestamps)

        bundles = create_temporal_bundles(df, window_ms=100)

        assert len(bundles) == 1
        assert bundles[0].request_count == 4

    def test_requests_separated_by_more_than_100ms(self):
        """Requests separated by >100ms should be in different bundles."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        timestamps = [
            base_time,
            base_time + timedelta(milliseconds=200),  # 200ms gap
            base_time + timedelta(milliseconds=400),  # 200ms gap
        ]
        df = self._create_requests_df(timestamps)

        bundles = create_temporal_bundles(df, window_ms=100)

        assert len(bundles) == 3
        for bundle in bundles:
            assert bundle.request_count == 1

    def test_mixed_clusters_and_singletons(self):
        """Mix of tight clusters and isolated requests."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        timestamps = [
            # Cluster 1: 3 requests within 100ms
            base_time,
            base_time + timedelta(milliseconds=30),
            base_time + timedelta(milliseconds=60),
            # Gap of 500ms
            # Cluster 2: 2 requests within 100ms
            base_time + timedelta(milliseconds=560),
            base_time + timedelta(milliseconds=600),
            # Gap of 300ms
            # Singleton
            base_time + timedelta(milliseconds=900),
        ]
        df = self._create_requests_df(timestamps)

        bundles = create_temporal_bundles(df, window_ms=100)

        assert len(bundles) == 3
        assert bundles[0].request_count == 3  # First cluster
        assert bundles[1].request_count == 2  # Second cluster
        assert bundles[2].request_count == 1  # Singleton

    def test_exact_100ms_boundary(self):
        """Request exactly at 100ms boundary should be included in bundle."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        timestamps = [
            base_time,
            base_time + timedelta(milliseconds=100),  # Exactly 100ms - included
        ]
        df = self._create_requests_df(timestamps)

        bundles = create_temporal_bundles(df, window_ms=100)

        # Exactly 100ms from bundle start is included (<=100ms check)
        assert len(bundles) == 1
        assert bundles[0].request_count == 2

    def test_just_over_100ms_boundary(self):
        """Request just over 100ms from bundle start should be in new bundle."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        timestamps = [
            base_time,
            base_time + timedelta(milliseconds=101),  # Just over 100ms
        ]
        df = self._create_requests_df(timestamps)

        bundles = create_temporal_bundles(df, window_ms=100)

        assert len(bundles) == 2
        assert bundles[0].request_count == 1
        assert bundles[1].request_count == 1

    def test_bundles_grouped_by_provider(self):
        """Bundles should be created separately for each provider."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        df = pd.DataFrame(
            {
                "datetime": [
                    base_time,
                    base_time + timedelta(milliseconds=20),
                    base_time + timedelta(milliseconds=40),
                    base_time + timedelta(milliseconds=60),
                ],
                "url": [f"https://example.com/page{i}" for i in range(4)],
                "bot_provider": ["OpenAI", "OpenAI", "Perplexity", "Perplexity"],
            }
        )

        bundles = create_temporal_bundles(df, window_ms=100, group_by="bot_provider")

        # Should have 2 bundles (one per provider), each with 2 requests
        assert len(bundles) == 2
        openai_bundle = [b for b in bundles if b.bot_provider == "OpenAI"][0]
        perplexity_bundle = [b for b in bundles if b.bot_provider == "Perplexity"][0]
        assert openai_bundle.request_count == 2
        assert perplexity_bundle.request_count == 2

    def test_bundle_metadata_calculated_correctly(self):
        """Bundle should have correct metadata (duration, URLs, etc.)."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        timestamps = [
            base_time,
            base_time + timedelta(milliseconds=30),
            base_time + timedelta(milliseconds=70),
        ]
        df = self._create_requests_df(timestamps)

        bundles = create_temporal_bundles(df, window_ms=100)

        assert len(bundles) == 1
        bundle = bundles[0]
        assert bundle.request_count == 3
        assert len(bundle.urls) == 3
        assert bundle.start_time == base_time
        assert bundle.end_time == timestamps[-1]
        assert bundle.duration_ms == 70  # 70ms total duration
        assert bundle.bot_provider == "OpenAI"

    def test_empty_dataframe_returns_empty_list(self):
        """Empty DataFrame should return empty bundle list."""
        df = pd.DataFrame(columns=["datetime", "url", "bot_provider"])

        bundles = create_temporal_bundles(df, window_ms=100)

        assert bundles == []

    def test_unsorted_requests_are_sorted(self):
        """Requests should be sorted by timestamp before bundling."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        # Create out-of-order timestamps
        timestamps = [
            base_time + timedelta(milliseconds=50),
            base_time,  # Earlier, but listed second
            base_time + timedelta(milliseconds=30),
        ]
        df = self._create_requests_df(timestamps)

        bundles = create_temporal_bundles(df, window_ms=100)

        # All should be in one bundle (sorted, all within 50ms)
        assert len(bundles) == 1
        assert bundles[0].request_count == 3
        assert bundles[0].start_time == base_time  # Should be earliest


class TestTemporalAnalyzer:
    """Tests for TemporalAnalyzer class."""

    def _create_test_df(self) -> pd.DataFrame:
        """Create test DataFrame with known patterns."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        return pd.DataFrame(
            {
                "datetime": [
                    base_time,
                    base_time + timedelta(milliseconds=20),
                    base_time + timedelta(milliseconds=50),
                    base_time + timedelta(milliseconds=500),  # Gap
                    base_time + timedelta(milliseconds=520),
                ],
                "url": [f"https://example.com/page{i}" for i in range(5)],
                "bot_provider": ["OpenAI"] * 5,
            }
        )

    def test_load_data_returns_self_for_chaining(self):
        """load_data should return self for method chaining."""
        analyzer = TemporalAnalyzer()
        df = self._create_test_df()

        result = analyzer.load_data(df)

        assert result is analyzer

    def test_create_bundles_uses_window_correctly(self):
        """create_bundles should use the specified window."""
        analyzer = TemporalAnalyzer()
        df = self._create_test_df()
        analyzer.load_data(df)

        bundles = analyzer.create_bundles(window_ms=100)

        # Should have 2 bundles: first 3 requests, then 2 requests
        assert len(bundles) == 2
        assert bundles[0].request_count == 3
        assert bundles[1].request_count == 2

    def test_create_bundles_with_optimal_window(self):
        """create_bundles should work with OPTIMAL_WINDOW_MS constant."""
        analyzer = TemporalAnalyzer()
        df = self._create_test_df()
        analyzer.load_data(df)

        bundles = analyzer.create_bundles(window_ms=OPTIMAL_WINDOW_MS)

        assert len(bundles) == 2

    def test_get_bundle_stats_returns_statistics(self):
        """get_bundle_stats should return correct statistics."""
        analyzer = TemporalAnalyzer()
        df = self._create_test_df()
        analyzer.load_data(df)

        stats = analyzer.get_bundle_stats(window_ms=100)

        assert stats["total_bundles"] == 2
        assert stats["total_requests"] == 5
        assert stats["mean_bundle_size"] == 2.5  # (3+2)/2


class TestComputeBundleStatistics:
    """Tests for compute_bundle_statistics function."""

    def test_empty_bundles_returns_zero_stats(self):
        """Empty bundle list should return zero statistics."""
        stats = compute_bundle_statistics([])

        assert stats["total_bundles"] == 0
        assert stats["total_requests"] == 0
        assert stats["singleton_rate"] == 0

    def test_singleton_rate_calculation(self):
        """Singleton rate should be correctly calculated."""
        bundles = [
            Bundle(
                bundle_id="1",
                start_time=datetime.now(),
                end_time=datetime.now(),
                request_count=1,
                bot_provider="OpenAI",
            ),
            Bundle(
                bundle_id="2",
                start_time=datetime.now(),
                end_time=datetime.now(),
                request_count=3,
                bot_provider="OpenAI",
            ),
        ]

        stats = compute_bundle_statistics(bundles)

        assert stats["total_bundles"] == 2
        assert stats["singleton_count"] == 1
        assert stats["singleton_rate"] == 0.5  # 1 out of 2

    def test_giant_bundle_detection(self):
        """Bundles with >10 requests should be counted as giants."""
        bundles = [
            Bundle(
                bundle_id="1",
                start_time=datetime.now(),
                end_time=datetime.now(),
                request_count=15,  # Giant
                bot_provider="OpenAI",
            ),
            Bundle(
                bundle_id="2",
                start_time=datetime.now(),
                end_time=datetime.now(),
                request_count=5,  # Normal
                bot_provider="OpenAI",
            ),
        ]

        stats = compute_bundle_statistics(bundles)

        assert stats["giant_count"] == 1
        assert stats["giant_rate"] == 0.5


class TestComputeInterRequestDeltas:
    """Tests for compute_inter_request_deltas function."""

    def test_computes_delta_between_requests(self):
        """Should compute time delta between consecutive requests."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        df = pd.DataFrame(
            {
                "datetime": [
                    base_time,
                    base_time + timedelta(milliseconds=100),
                    base_time + timedelta(milliseconds=250),
                ],
                "bot_provider": ["OpenAI"] * 3,
            }
        )

        result = compute_inter_request_deltas(df)

        assert "delta_ms" in result.columns
        # First row should have NaN delta
        assert pd.isna(result.iloc[0]["delta_ms"])
        # Second row: 100ms from first
        assert result.iloc[1]["delta_ms"] == 100
        # Third row: 150ms from second
        assert result.iloc[2]["delta_ms"] == 150


class TestBundleDataclass:
    """Tests for Bundle dataclass."""

    def test_duration_ms_property(self):
        """duration_ms should calculate correct duration."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = start + timedelta(milliseconds=75)

        bundle = Bundle(
            bundle_id="test",
            start_time=start,
            end_time=end,
            request_count=3,
            bot_provider="OpenAI",
        )

        assert bundle.duration_ms == 75
