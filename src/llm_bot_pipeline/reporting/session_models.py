"""Data models for query fan-out session reporting."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SessionRecord:
    """A query fan-out session record for storage."""

    session_id: str
    session_date: str
    session_start_time: str
    session_end_time: str
    duration_ms: float
    bot_provider: str
    bot_name: Optional[str]
    request_count: int
    unique_urls: int
    mean_cosine_similarity: Optional[float]
    min_cosine_similarity: Optional[float]
    max_cosine_similarity: Optional[float]
    confidence_level: str
    fanout_session_name: Optional[str]
    url_list: str
    window_ms: float
    domain: Optional[str] = None
    splitting_strategy: Optional[str] = None
    _created_at: str = ""
    parent_session_id: Optional[str] = None
    was_refined: bool = False
    refinement_reason: Optional[str] = None
    pre_refinement_mibcs: Optional[float] = None


@dataclass
class RefinementMetrics:
    """Metrics from session refinement stage."""

    enabled: bool = False
    bundles_analyzed: int = 0
    collision_candidates: int = 0
    bundles_split: int = 0
    sub_bundles_created: int = 0
    mean_mibcs_improvement: float = 0.0

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "enabled": self.enabled,
            "bundles_analyzed": self.bundles_analyzed,
            "collision_candidates": self.collision_candidates,
            "bundles_split": self.bundles_split,
            "sub_bundles_created": self.sub_bundles_created,
            "mean_mibcs_improvement": self.mean_mibcs_improvement,
        }


@dataclass
class SessionAggregationResult:
    """Result of session aggregation operation."""

    success: bool
    sessions_created: int
    total_requests_bundled: int
    window_ms: float
    mean_session_size: float
    high_confidence_count: int
    medium_confidence_count: int
    low_confidence_count: int
    error: Optional[str] = None
    duration_seconds: float = 0.0
    refinement: RefinementMetrics = field(default_factory=RefinementMetrics)
