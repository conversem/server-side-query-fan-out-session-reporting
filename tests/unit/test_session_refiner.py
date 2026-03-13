"""Tests for SessionRefiner class."""

from datetime import datetime, timedelta, timezone

import pytest

from llm_bot_pipeline.reporting.session_refiner import (
    QualityScore,
    RefinedSession,
    RefinerResult,
    SessionRefiner,
)
from llm_bot_pipeline.reporting.temporal_bundler import BundleResult
from llm_bot_pipeline.research.temporal_analysis import Bundle


def _make_bundle(
    bundle_id: str = "b1",
    start: str = "2025-01-01T00:00:00",
    end: str = "2025-01-01T00:00:05",
    urls: list[str] | None = None,
    provider: str = "openai",
    request_count: int | None = None,
) -> Bundle:
    """Helper to build a Bundle with sensible defaults."""
    st = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    et = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    urls = urls or [f"https://example.com/p{i}" for i in range(3)]
    return Bundle(
        bundle_id=bundle_id,
        start_time=st,
        end_time=et,
        request_count=request_count if request_count is not None else len(urls),
        bot_provider=provider,
        urls=urls,
        request_indices=list(range(len(urls))),
    )


def _make_bundle_result(bundles: list[Bundle]) -> BundleResult:
    total = sum(b.request_count for b in bundles)
    providers = sorted({b.bot_provider for b in bundles})
    return BundleResult(
        bundles=bundles,
        total_requests=total,
        window_ms=5000.0,
        providers_processed=providers,
    )


class TestSessionRefinerMergesAdjacent:
    """Feed adjacent bundles and assert merged session output."""

    def test_adjacent_small_bundles_merged(self):
        b1 = _make_bundle(
            bundle_id="b1",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:01",
            urls=["https://example.com/a"],
        )
        b2 = _make_bundle(
            bundle_id="b2",
            start="2025-01-01T00:00:01.500",
            end="2025-01-01T00:00:03",
            urls=["https://example.com/b", "https://example.com/c"],
        )

        refiner = SessionRefiner(merge_gap_ms=2000, min_bundle_size=2)
        result = refiner.refine(_make_bundle_result([b1, b2]))

        assert result.merged_count == 1
        assert result.final_count == 1
        merged = result.sessions[0]
        assert merged.bundle.request_count == 3
        assert len(merged.bundle.urls) == 3

    def test_no_merge_when_gap_too_large(self):
        b1 = _make_bundle(
            bundle_id="b1",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:01",
            urls=["https://example.com/a"],
        )
        b2 = _make_bundle(
            bundle_id="b2",
            start="2025-01-01T00:00:10",
            end="2025-01-01T00:00:12",
            urls=["https://example.com/b", "https://example.com/c"],
        )

        refiner = SessionRefiner(merge_gap_ms=2000, min_bundle_size=2)
        result = refiner.refine(_make_bundle_result([b1, b2]))

        assert result.merged_count == 0
        assert result.final_count == 2

    def test_no_merge_different_providers(self):
        b1 = _make_bundle(
            bundle_id="b1",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:01",
            urls=["https://example.com/a"],
            provider="openai",
        )
        b2 = _make_bundle(
            bundle_id="b2",
            start="2025-01-01T00:00:01.500",
            end="2025-01-01T00:00:03",
            urls=["https://example.com/b", "https://example.com/c"],
            provider="anthropic",
        )

        refiner = SessionRefiner(merge_gap_ms=2000, min_bundle_size=2)
        result = refiner.refine(_make_bundle_result([b1, b2]))

        assert result.merged_count == 0
        assert result.final_count == 2

    def test_no_merge_when_first_already_large(self):
        b1 = _make_bundle(
            bundle_id="b1",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:01",
            urls=["https://example.com/a", "https://example.com/b"],
        )
        b2 = _make_bundle(
            bundle_id="b2",
            start="2025-01-01T00:00:01.500",
            end="2025-01-01T00:00:03",
            urls=["https://example.com/c"],
        )

        refiner = SessionRefiner(merge_gap_ms=2000, min_bundle_size=2)
        result = refiner.refine(_make_bundle_result([b1, b2]))

        assert result.merged_count == 0
        assert result.final_count == 2


class TestSessionRefinerSplitsLong:
    """Feed oversized session, assert split."""

    def test_split_by_request_count(self):
        urls = [f"https://example.com/p{i}" for i in range(60)]
        big = _make_bundle(
            bundle_id="big",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:10",
            urls=urls,
        )

        refiner = SessionRefiner(max_bundle_size=50)
        result = refiner.refine(_make_bundle_result([big]))

        assert result.split_count >= 1
        assert result.final_count >= 2
        total_urls = sum(len(s.bundle.urls) for s in result.sessions)
        assert total_urls == 60

    def test_split_by_duration(self):
        bundle = _make_bundle(
            bundle_id="long",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:02:00",
            urls=[f"https://example.com/p{i}" for i in range(4)],
        )

        refiner = SessionRefiner(max_duration_ms=60000)
        result = refiner.refine(_make_bundle_result([bundle]))

        assert result.split_count >= 1
        assert result.final_count >= 2

    def test_no_split_when_within_limits(self):
        bundle = _make_bundle(
            bundle_id="ok",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:05",
            urls=[f"https://example.com/p{i}" for i in range(5)],
        )

        refiner = SessionRefiner(max_bundle_size=50, max_duration_ms=60000)
        result = refiner.refine(_make_bundle_result([bundle]))

        assert result.split_count == 0
        assert result.final_count == 1


class TestQualityScoreCalculation:
    """Test scoring algorithm."""

    def test_ideal_bundle_scores_high(self):
        bundle = _make_bundle(
            bundle_id="ideal",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:05",
            urls=[f"https://example.com/p{i}" for i in range(5)],
        )

        refiner = SessionRefiner()
        scores = refiner.score_bundles([bundle])

        assert len(scores) == 1
        score = scores[0]
        assert score.url_diversity == 1.0
        assert score.size_score == 1.0
        assert score.overall > 0.5
        assert score.flags == []

    def test_single_request_flagged(self):
        bundle = _make_bundle(
            bundle_id="single",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:00",
            urls=["https://example.com/p1"],
        )

        refiner = SessionRefiner()
        scores = refiner.score_bundles([bundle])
        score = scores[0]

        assert "single_request" in score.flags
        assert score.size_score < 1.0

    def test_duplicate_urls_lower_diversity(self):
        bundle = _make_bundle(
            bundle_id="dups",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:05",
            urls=["https://example.com/a"] * 10,
            request_count=10,
        )

        refiner = SessionRefiner()
        scores = refiner.score_bundles([bundle])
        score = scores[0]

        assert score.url_diversity == 0.1
        assert "low_url_diversity" in score.flags

    def test_oversized_bundle_flagged(self):
        urls = [f"https://example.com/p{i}" for i in range(100)]
        bundle = _make_bundle(
            bundle_id="huge",
            start="2025-01-01T00:00:00",
            end="2025-01-01T00:00:10",
            urls=urls,
        )

        refiner = SessionRefiner(max_bundle_size=50)
        scores = refiner.score_bundles([bundle])
        assert "oversized" in scores[0].flags

    def test_quality_score_to_dict(self):
        bundle = _make_bundle()
        refiner = SessionRefiner()
        score = refiner.score_bundles([bundle])[0]
        d = score.to_dict()
        assert "overall" in d
        assert "flags" in d
        assert isinstance(d["flags"], list)


class TestEmptyInputHandling:
    """Handle empty bundle result."""

    def test_empty_bundle_result(self):
        empty = BundleResult(
            bundles=[],
            total_requests=0,
            window_ms=5000.0,
            providers_processed=[],
        )

        refiner = SessionRefiner()
        result = refiner.refine(empty)

        assert result.final_count == 0
        assert result.original_count == 0
        assert result.merged_count == 0
        assert result.split_count == 0
        assert result.mean_quality == 0.0
        assert result.summary()["final_count"] == 0

    def test_single_bundle_passes_through(self):
        bundle = _make_bundle()
        refiner = SessionRefiner()
        result = refiner.refine(_make_bundle_result([bundle]))

        assert result.final_count == 1
        assert result.merged_count == 0
        assert result.split_count == 0

    def test_score_bundles_empty_list(self):
        refiner = SessionRefiner()
        assert refiner.score_bundles([]) == []


class TestRefinerResultSummary:
    """Test RefinerResult properties and summary."""

    def test_summary_keys(self):
        bundle = _make_bundle()
        refiner = SessionRefiner()
        result = refiner.refine(_make_bundle_result([bundle]))
        s = result.summary()

        assert set(s.keys()) == {
            "original_count",
            "final_count",
            "merged_count",
            "split_count",
            "mean_quality",
        }

    def test_mean_quality_within_bounds(self):
        bundles = [
            _make_bundle(
                bundle_id=f"b{i}",
                urls=[f"https://example.com/p{j}" for j in range(3)],
            )
            for i in range(5)
        ]
        refiner = SessionRefiner()
        result = refiner.refine(_make_bundle_result(bundles))

        assert 0.0 <= result.mean_quality <= 1.0
