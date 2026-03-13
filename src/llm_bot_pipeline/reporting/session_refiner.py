"""
Session refinement for the reporting pipeline.

Provides merging, splitting, and quality scoring of temporal bundles
before they are stored as session records. Works with BundleResult
from TemporalBundler and prepares refined sessions for storage.

Used by SessionAggregator (future SessionAggregator integration) as the
Stage 2 refinement step between temporal bundling and storage.

Three refinement stages:
1. Merge: Combine adjacent small bundles from the same provider
2. Split: Break apart oversized sessions
3. Score: Assess quality of each resulting session
"""

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional

from ..research.temporal_analysis import Bundle
from .temporal_bundler import BundleResult

logger = logging.getLogger(__name__)


# =============================================================================
# Data classes
# =============================================================================


@dataclass
class QualityScore:
    """Quality assessment for a session bundle."""

    bundle_id: str
    url_diversity: float
    duration_score: float
    size_score: float
    overall: float
    flags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "bundle_id": self.bundle_id,
            "url_diversity": self.url_diversity,
            "duration_score": self.duration_score,
            "size_score": self.size_score,
            "overall": self.overall,
            "flags": self.flags,
        }


@dataclass
class RefinedSession:
    """A session bundle after refinement processing."""

    bundle: Bundle
    quality: QualityScore
    was_merged: bool = False
    was_split: bool = False
    source_bundle_ids: list[str] = field(default_factory=list)

    @property
    def bundle_id(self) -> str:
        return self.bundle.bundle_id


@dataclass
class RefinerResult:
    """Result of the full refinement pipeline."""

    sessions: list[RefinedSession]
    original_count: int
    merged_count: int
    split_count: int

    @property
    def final_count(self) -> int:
        return len(self.sessions)

    @property
    def mean_quality(self) -> float:
        if not self.sessions:
            return 0.0
        return sum(s.quality.overall for s in self.sessions) / len(self.sessions)

    def summary(self) -> dict:
        return {
            "original_count": self.original_count,
            "final_count": self.final_count,
            "merged_count": self.merged_count,
            "split_count": self.split_count,
            "mean_quality": round(self.mean_quality, 4),
        }


# =============================================================================
# SessionRefiner
# =============================================================================


class SessionRefiner:
    """
    Refines temporal bundles by merging, splitting, and scoring.

    Works with output from TemporalBundler (BundleResult) and produces
    RefinedSession objects ready for storage conversion.

    Pipeline: merge adjacent -> split oversized -> score quality.
    """

    def __init__(
        self,
        merge_gap_ms: float = 2000.0,
        min_bundle_size: int = 2,
        max_bundle_size: int = 50,
        max_duration_ms: float = 60000.0,
        ideal_size_range: tuple[int, int] = (2, 20),
        ideal_duration_range_ms: tuple[float, float] = (500.0, 30000.0),
    ):
        """
        Args:
            merge_gap_ms: Max gap between bundles to consider merging (ms).
            min_bundle_size: Bundles smaller than this are merge candidates.
            max_bundle_size: Maximum requests before splitting is triggered.
            max_duration_ms: Maximum duration before splitting is triggered (ms).
            ideal_size_range: (min, max) request count for ideal quality score.
            ideal_duration_range_ms: (min, max) duration in ms for ideal score.
        """
        self.merge_gap_ms = merge_gap_ms
        self.min_bundle_size = min_bundle_size
        self.max_bundle_size = max_bundle_size
        self.max_duration_ms = max_duration_ms
        self.ideal_size_range = ideal_size_range
        self.ideal_duration_range_ms = ideal_duration_range_ms

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    def refine(self, bundle_result: BundleResult) -> RefinerResult:
        """
        Apply full refinement pipeline to a bundle result.

        Pipeline: merge adjacent -> split oversized -> score quality.

        Args:
            bundle_result: Output from TemporalBundler.

        Returns:
            RefinerResult with refined sessions and metrics.
        """
        if not bundle_result.bundles:
            return RefinerResult(
                sessions=[],
                original_count=0,
                merged_count=0,
                split_count=0,
            )

        original_count = len(bundle_result.bundles)

        merged_bundles, merge_count = self._merge_adjacent(bundle_result.bundles)
        split_bundles, split_count = self._split_oversized(merged_bundles)

        sessions = []
        for b in split_bundles:
            quality = self._score_quality(b)
            was_merged = "_merged" in b.bundle_id
            was_split = "_split_" in b.bundle_id
            source_ids = []
            if was_merged:
                source_ids = [b.bundle_id.replace("_merged", "")]
            sessions.append(
                RefinedSession(
                    bundle=b,
                    quality=quality,
                    was_merged=was_merged,
                    was_split=was_split,
                    source_bundle_ids=source_ids,
                )
            )

        logger.info(
            "Refinement complete: %d -> %d sessions "
            "(merged=%d, split=%d, mean_quality=%.3f)",
            original_count,
            len(sessions),
            merge_count,
            split_count,
            (
                sum(s.quality.overall for s in sessions) / len(sessions)
                if sessions
                else 0.0
            ),
        )

        return RefinerResult(
            sessions=sessions,
            original_count=original_count,
            merged_count=merge_count,
            split_count=split_count,
        )

    def score_bundles(self, bundles: list[Bundle]) -> list[QualityScore]:
        """
        Score quality of bundles without merge/split.

        Useful for analysis/reporting without modifying the bundles.

        Args:
            bundles: List of Bundle objects to score.

        Returns:
            List of QualityScore objects.
        """
        return [self._score_quality(b) for b in bundles]

    # -----------------------------------------------------------------
    # Stage 1: Merge adjacent small bundles
    # -----------------------------------------------------------------

    def _merge_adjacent(self, bundles: list[Bundle]) -> tuple[list[Bundle], int]:
        """
        Merge adjacent bundles that are close in time and from the same provider.

        Two bundles are merged when:
        - Same bot_provider
        - Gap between end of first and start of second <= merge_gap_ms
        - First bundle has fewer than min_bundle_size requests

        Returns:
            Tuple of (merged bundles, count of merges performed).
        """
        if len(bundles) <= 1:
            return list(bundles), 0

        sorted_bundles = sorted(bundles, key=lambda b: (b.bot_provider, b.start_time))

        merged: list[Bundle] = []
        merge_count = 0
        current = sorted_bundles[0]

        for next_bundle in sorted_bundles[1:]:
            if self._should_merge(current, next_bundle):
                current = self._do_merge(current, next_bundle)
                merge_count += 1
            else:
                merged.append(current)
                current = next_bundle

        merged.append(current)
        return merged, merge_count

    def _should_merge(self, a: Bundle, b: Bundle) -> bool:
        if a.bot_provider != b.bot_provider:
            return False

        gap_ms = (b.start_time - a.end_time).total_seconds() * 1000
        if gap_ms > self.merge_gap_ms:
            return False

        return a.request_count < self.min_bundle_size

    def _do_merge(self, a: Bundle, b: Bundle) -> Bundle:
        return Bundle(
            bundle_id=f"{a.bundle_id}_merged",
            start_time=min(a.start_time, b.start_time),
            end_time=max(a.end_time, b.end_time),
            request_count=a.request_count + b.request_count,
            bot_provider=a.bot_provider,
            urls=a.urls + b.urls,
            request_indices=a.request_indices + b.request_indices,
        )

    # -----------------------------------------------------------------
    # Stage 2: Split oversized bundles
    # -----------------------------------------------------------------

    def _split_oversized(self, bundles: list[Bundle]) -> tuple[list[Bundle], int]:
        """
        Split bundles that exceed size or duration thresholds.

        Returns:
            Tuple of (resulting bundles, count of splits performed).
        """
        result: list[Bundle] = []
        split_count = 0

        for bundle in bundles:
            if self._needs_split(bundle):
                sub_bundles = self._do_split(bundle)
                result.extend(sub_bundles)
                split_count += 1
            else:
                result.append(bundle)

        return result, split_count

    def _needs_split(self, bundle: Bundle) -> bool:
        if bundle.request_count > self.max_bundle_size:
            return True
        if bundle.duration_ms > self.max_duration_ms:
            return True
        return False

    def _do_split(self, bundle: Bundle) -> list[Bundle]:
        """Split an oversized bundle into roughly equal halves (recursive)."""
        mid = bundle.request_count // 2
        if mid < 1:
            return [bundle]

        urls_a = bundle.urls[:mid]
        urls_b = bundle.urls[mid:]
        indices_a = bundle.request_indices[:mid]
        indices_b = bundle.request_indices[mid:]

        mid_offset = timedelta(milliseconds=bundle.duration_ms / 2)
        mid_time = bundle.start_time + mid_offset

        first = Bundle(
            bundle_id=f"{bundle.bundle_id}_split_0",
            start_time=bundle.start_time,
            end_time=mid_time,
            request_count=len(urls_a),
            bot_provider=bundle.bot_provider,
            urls=urls_a,
            request_indices=indices_a,
        )
        second = Bundle(
            bundle_id=f"{bundle.bundle_id}_split_1",
            start_time=mid_time,
            end_time=bundle.end_time,
            request_count=len(urls_b),
            bot_provider=bundle.bot_provider,
            urls=urls_b,
            request_indices=indices_b,
        )

        result: list[Bundle] = []
        for sub in (first, second):
            if self._needs_split(sub):
                result.extend(self._do_split(sub))
            else:
                result.append(sub)

        return result

    # -----------------------------------------------------------------
    # Stage 3: Quality scoring
    # -----------------------------------------------------------------

    def _score_quality(self, bundle: Bundle) -> QualityScore:
        """
        Calculate quality score for a bundle.

        Components (each 0.0 – 1.0):
        - url_diversity: unique URLs / total URLs
        - duration_score: 1.0 inside ideal range, degrades outside
        - size_score: 1.0 inside ideal range, degrades outside
        - overall: 0.4 * size + 0.3 * diversity + 0.3 * duration
        """
        unique_urls = len(set(bundle.urls)) if bundle.urls else 0
        total_urls = len(bundle.urls) if bundle.urls else 0
        url_diversity = unique_urls / total_urls if total_urls > 0 else 0.0

        duration_score = self._range_score(
            bundle.duration_ms, *self.ideal_duration_range_ms
        )
        size_score = self._range_score(
            float(bundle.request_count), *self.ideal_size_range
        )

        overall = 0.4 * size_score + 0.3 * url_diversity + 0.3 * duration_score

        flags: list[str] = []
        if total_urls > 0 and url_diversity < 0.5:
            flags.append("low_url_diversity")
        if bundle.request_count == 1:
            flags.append("single_request")
        if bundle.request_count > self.max_bundle_size:
            flags.append("oversized")
        if bundle.duration_ms > self.max_duration_ms:
            flags.append("long_duration")

        return QualityScore(
            bundle_id=bundle.bundle_id,
            url_diversity=round(url_diversity, 4),
            duration_score=round(duration_score, 4),
            size_score=round(size_score, 4),
            overall=round(overall, 4),
            flags=flags,
        )

    @staticmethod
    def _range_score(value: float, lo: float, hi: float) -> float:
        """Return 1.0 when *value* is in [lo, hi], degrading linearly outside."""
        if lo <= value <= hi:
            return 1.0
        if value < lo:
            return max(0.0, value / lo) if lo > 0 else 0.0
        # value > hi
        return max(0.0, min(1.0, 1.0 - (value - hi) / hi)) if hi > 0 else 0.0
