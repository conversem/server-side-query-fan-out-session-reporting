"""
Fingerprint analysis for IP homogeneity and request attribute consistency.

Provides tools for analyzing IP distribution, response patterns, and detecting
potential collision issues in temporal bundles.

This module builds on EnrichedBundle from temporal_analysis.py and provides
higher-level aggregation and analysis capabilities.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from .temporal_analysis import EnrichedBundle

logger = logging.getLogger(__name__)

__all__ = [
    "IPHomogeneityMetrics",
    "FingerprintConsistencyMetrics",
    "CollisionCandidate",
    "BundlingComparisonMetrics",
    "FingerprintAnalyzer",
]


# =============================================================================
# Metrics Dataclasses
# =============================================================================


@dataclass
class IPHomogeneityMetrics:
    """
    Aggregated IP homogeneity metrics across multiple bundles.

    Attributes:
        total_bundles: Number of bundles analyzed
        bundles_with_single_ip: Bundles where all requests share one IP
        bundles_with_single_subnet: Bundles within a single /24 subnet
        mean_ip_homogeneity: Average IP homogeneity score (0-1)
        mean_subnet_homogeneity: Average subnet homogeneity score (0-1)
        mean_unique_ips_per_bundle: Average number of unique IPs per bundle
        mean_unique_subnets_per_bundle: Average number of unique subnets per bundle
        ip_homogeneity_rate: Fraction of bundles with perfect IP homogeneity (1.0)
        subnet_homogeneity_rate: Fraction of bundles with perfect subnet homogeneity
    """

    total_bundles: int
    bundles_with_single_ip: int
    bundles_with_single_subnet: int
    mean_ip_homogeneity: float
    mean_subnet_homogeneity: float
    mean_unique_ips_per_bundle: float
    mean_unique_subnets_per_bundle: float
    ip_homogeneity_rate: float
    subnet_homogeneity_rate: float

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "total_bundles": self.total_bundles,
            "bundles_with_single_ip": self.bundles_with_single_ip,
            "bundles_with_single_subnet": self.bundles_with_single_subnet,
            "mean_ip_homogeneity": self.mean_ip_homogeneity,
            "mean_subnet_homogeneity": self.mean_subnet_homogeneity,
            "mean_unique_ips_per_bundle": self.mean_unique_ips_per_bundle,
            "mean_unique_subnets_per_bundle": self.mean_unique_subnets_per_bundle,
            "ip_homogeneity_rate": self.ip_homogeneity_rate,
            "subnet_homogeneity_rate": self.subnet_homogeneity_rate,
        }


@dataclass
class FingerprintConsistencyMetrics:
    """
    Aggregated fingerprint consistency metrics across multiple bundles.

    Focuses on geographic fingerprints (country) as the primary consistency
    indicator beyond IP/subnet. BotTags and ResponseStatus are excluded
    as they don't reflect user-agent request characteristics.

    Attributes:
        total_bundles: Number of bundles analyzed
        mean_country_consistency: Average country consistency (0-1)
    """

    total_bundles: int
    mean_country_consistency: float

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "total_bundles": self.total_bundles,
            "mean_country_consistency": self.mean_country_consistency,
        }


@dataclass
class CollisionCandidate:
    """
    A bundle identified as a potential collision (multiple sources bundled together).

    Attributes:
        bundle: The EnrichedBundle that may be a collision
        ip_count: Number of unique IPs in the bundle
        subnet_count: Number of unique /24 subnets
        ip_homogeneity: IP homogeneity score
        coherence_score: Semantic coherence (if available)
        collision_score: Overall collision likelihood (0-1, higher = more likely)
        reason: Human-readable explanation for collision detection
    """

    bundle: EnrichedBundle
    ip_count: int
    subnet_count: int
    ip_homogeneity: float
    coherence_score: Optional[float]
    collision_score: float
    reason: str

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization (excludes bundle object)."""
        return {
            "bundle_id": self.bundle.bundle_id,
            "ip_count": self.ip_count,
            "subnet_count": self.subnet_count,
            "ip_homogeneity": self.ip_homogeneity,
            "coherence_score": self.coherence_score,
            "collision_score": self.collision_score,
            "reason": self.reason,
        }


@dataclass
class BundlingComparisonMetrics:
    """
    Metrics comparing IP-constrained vs temporal-only bundling.

    Attributes:
        temporal_bundle_count: Bundles from temporal-only approach
        ip_constrained_bundle_count: Bundles from IP-constrained approach
        bundle_count_change_pct: Percentage change in bundle count
        mean_bundle_size_temporal: Average size of temporal bundles
        mean_bundle_size_ip_constrained: Average size of IP-constrained bundles
        collisions_in_temporal: Number of potential collisions in temporal approach
        improvement_potential: Estimated improvement from IP constraints
    """

    temporal_bundle_count: int
    ip_constrained_bundle_count: int
    bundle_count_change_pct: float
    mean_bundle_size_temporal: float
    mean_bundle_size_ip_constrained: float
    collisions_in_temporal: int
    improvement_potential: float

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "temporal_bundle_count": self.temporal_bundle_count,
            "ip_constrained_bundle_count": self.ip_constrained_bundle_count,
            "bundle_count_change_pct": self.bundle_count_change_pct,
            "mean_bundle_size_temporal": self.mean_bundle_size_temporal,
            "mean_bundle_size_ip_constrained": self.mean_bundle_size_ip_constrained,
            "collisions_in_temporal": self.collisions_in_temporal,
            "improvement_potential": self.improvement_potential,
        }


# =============================================================================
# Fingerprint Analyzer
# =============================================================================


class FingerprintAnalyzer:
    """
    Analyzer for IP homogeneity and fingerprint consistency in temporal bundles.

    Provides methods to:
    - Compute aggregated IP homogeneity metrics across bundles
    - Compute fingerprint consistency metrics (response status, country, etc.)
    - Detect potential collisions (bundles with multiple sources)
    - Evaluate IP-constrained vs temporal-only bundling approaches

    Example:
        >>> analyzer = FingerprintAnalyzer()
        >>> bundles = [EnrichedBundle(...), ...]
        >>> ip_metrics = analyzer.compute_ip_homogeneity(bundles)
        >>> print(f"IP homogeneity rate: {ip_metrics.ip_homogeneity_rate:.1%}")
    """

    def __init__(
        self,
        collision_ip_threshold: int = 2,
        collision_homogeneity_threshold: float = 0.5,
    ):
        """
        Initialize fingerprint analyzer.

        Args:
            collision_ip_threshold: Minimum unique IPs to flag as potential collision
            collision_homogeneity_threshold: IP homogeneity below this flags collision
        """
        self.collision_ip_threshold = collision_ip_threshold
        self.collision_homogeneity_threshold = collision_homogeneity_threshold

    def compute_ip_homogeneity(
        self, bundles: list[EnrichedBundle]
    ) -> IPHomogeneityMetrics:
        """
        Compute aggregated IP homogeneity metrics across bundles.

        Args:
            bundles: List of EnrichedBundle objects to analyze

        Returns:
            IPHomogeneityMetrics with aggregated statistics
        """
        if not bundles:
            return IPHomogeneityMetrics(
                total_bundles=0,
                bundles_with_single_ip=0,
                bundles_with_single_subnet=0,
                mean_ip_homogeneity=1.0,
                mean_subnet_homogeneity=1.0,
                mean_unique_ips_per_bundle=0.0,
                mean_unique_subnets_per_bundle=0.0,
                ip_homogeneity_rate=1.0,
                subnet_homogeneity_rate=1.0,
            )

        ip_homogeneities = []
        subnet_homogeneities = []
        unique_ip_counts = []
        unique_subnet_counts = []
        single_ip_count = 0
        single_subnet_count = 0

        for bundle in bundles:
            ip_hom = bundle.ip_homogeneity
            subnet_hom = bundle.subnet_homogeneity
            unique_ips = len(bundle.unique_ips)
            unique_subnets = len(bundle.unique_subnets_24)

            ip_homogeneities.append(ip_hom)
            subnet_homogeneities.append(subnet_hom)
            unique_ip_counts.append(unique_ips)
            unique_subnet_counts.append(unique_subnets)

            if ip_hom == 1.0:
                single_ip_count += 1
            if subnet_hom == 1.0:
                single_subnet_count += 1

        total = len(bundles)
        return IPHomogeneityMetrics(
            total_bundles=total,
            bundles_with_single_ip=single_ip_count,
            bundles_with_single_subnet=single_subnet_count,
            mean_ip_homogeneity=float(np.mean(ip_homogeneities)),
            mean_subnet_homogeneity=float(np.mean(subnet_homogeneities)),
            mean_unique_ips_per_bundle=float(np.mean(unique_ip_counts)),
            mean_unique_subnets_per_bundle=float(np.mean(unique_subnet_counts)),
            ip_homogeneity_rate=single_ip_count / total,
            subnet_homogeneity_rate=single_subnet_count / total,
        )

    def compute_fingerprint_consistency(
        self, bundles: list[EnrichedBundle]
    ) -> FingerprintConsistencyMetrics:
        """
        Compute aggregated fingerprint consistency metrics across bundles.

        Focuses on country consistency as the primary fingerprint beyond IP.
        BotTags and ResponseStatus are excluded as they don't reflect
        user-agent request characteristics.

        Args:
            bundles: List of EnrichedBundle objects to analyze

        Returns:
            FingerprintConsistencyMetrics with country consistency
        """
        if not bundles:
            return FingerprintConsistencyMetrics(
                total_bundles=0,
                mean_country_consistency=1.0,
            )

        country_consistencies = []
        for bundle in bundles:
            country_consistencies.append(bundle.country_consistency())

        mean_country = float(np.mean(country_consistencies))

        return FingerprintConsistencyMetrics(
            total_bundles=len(bundles),
            mean_country_consistency=mean_country,
        )

    def detect_collisions(
        self,
        bundles: list[EnrichedBundle],
        coherence_scores: dict[str, float],
        mibcs_threshold: float = 0.5,
    ) -> list[CollisionCandidate]:
        """
        Detect bundles that may be collisions (semantically incoherent bundles).

        A collision occurs when a temporal bundle contains requests from multiple
        independent user queries that happened to occur within the time window,
        resulting in thematically unrelated URLs being grouped together.

        The collision signal is LOW MIBCS (semantic incoherence). IP diversity
        is recorded as correlation data to determine if it's a useful signal.

        Prerequisites:
        - Bundle must have ≥2 unique URLs (otherwise can't measure coherence)
        - Bundle must have a computed MIBCS score

        Args:
            bundles: List of EnrichedBundle objects to analyze
            coherence_scores: Dict mapping bundle_id to MIBCS score (required)
            mibcs_threshold: MIBCS below this threshold = collision (default 0.5)

        Returns:
            List of CollisionCandidate objects for potential collisions
        """
        collisions = []

        for bundle in bundles:
            # Prerequisite: need ≥2 unique URLs for semantic coherence analysis
            unique_urls = len(set(bundle.urls))
            if unique_urls < 2:
                continue

            # Get MIBCS score - required for collision detection
            mibcs = coherence_scores.get(bundle.bundle_id)
            if mibcs is None:
                continue

            # Collision = low semantic coherence (the established signal)
            if mibcs >= mibcs_threshold:
                continue  # Not a collision - URLs are semantically coherent

            # This is a collision candidate (low MIBCS)
            ip_count = len(bundle.unique_ips)
            subnet_count = len(bundle.unique_subnets_24)
            ip_hom = bundle.ip_homogeneity

            # Build reason - primary is semantic incoherence
            reasons = [f"Low MIBCS ({mibcs:.3f} < {mibcs_threshold})"]
            if ip_count >= 2:
                reasons.append(f"{ip_count} unique IPs")
            if subnet_count >= 2:
                reasons.append(f"{subnet_count} unique subnets")

            # Collision score based primarily on semantic incoherence
            collision_score = 1.0 - mibcs  # Lower MIBCS = higher collision score

            collisions.append(
                CollisionCandidate(
                    bundle=bundle,
                    ip_count=ip_count,
                    subnet_count=subnet_count,
                    ip_homogeneity=ip_hom,
                    coherence_score=mibcs,
                    collision_score=collision_score,
                    reason="; ".join(reasons),
                )
            )

        # Sort by collision score (highest first = lowest MIBCS)
        collisions.sort(key=lambda c: c.collision_score, reverse=True)

        return collisions

    def evaluate_ip_constrained_bundling(
        self,
        temporal_bundles: list[EnrichedBundle],
        ip_constrained_bundles: list[EnrichedBundle],
        coherence_scores: Optional[dict[str, float]] = None,
    ) -> BundlingComparisonMetrics:
        """
        Compare IP-constrained bundling vs temporal-only bundling.

        IP-constrained bundling adds the requirement that all requests in a
        bundle must share the same IP (or subnet), potentially breaking up
        bundles that were grouped only by time.

        Args:
            temporal_bundles: Bundles from temporal-only approach
            ip_constrained_bundles: Bundles from IP-constrained approach
            coherence_scores: Optional dict mapping bundle_id to MIBCS score

        Returns:
            BundlingComparisonMetrics comparing the two approaches
        """
        temporal_count = len(temporal_bundles)
        ip_count = len(ip_constrained_bundles)

        # Calculate bundle count change
        if temporal_count > 0:
            change_pct = (ip_count - temporal_count) / temporal_count * 100
        else:
            change_pct = 0.0

        # Mean bundle sizes
        temporal_sizes = [b.request_count for b in temporal_bundles]
        ip_sizes = [b.request_count for b in ip_constrained_bundles]

        mean_temporal_size = float(np.mean(temporal_sizes)) if temporal_sizes else 0.0
        mean_ip_size = float(np.mean(ip_sizes)) if ip_sizes else 0.0

        # Count collisions in temporal approach (requires coherence scores)
        collision_count = 0
        if coherence_scores:
            collisions = self.detect_collisions(temporal_bundles, coherence_scores)
            collision_count = len(collisions)

        # Estimate improvement potential
        # (reduction in collisions relative to total bundles)
        if temporal_count > 0:
            improvement = collision_count / temporal_count
        else:
            improvement = 0.0

        return BundlingComparisonMetrics(
            temporal_bundle_count=temporal_count,
            ip_constrained_bundle_count=ip_count,
            bundle_count_change_pct=change_pct,
            mean_bundle_size_temporal=mean_temporal_size,
            mean_bundle_size_ip_constrained=mean_ip_size,
            collisions_in_temporal=collision_count,
            improvement_potential=improvement,
        )

    def analyze_bundles(
        self,
        bundles: list[EnrichedBundle],
        coherence_scores: dict[str, float],
    ) -> dict:
        """
        Perform comprehensive fingerprint analysis on bundles.

        Combines all analysis methods into a single report.

        Args:
            bundles: List of EnrichedBundle objects to analyze
            coherence_scores: Dict mapping bundle_id to MIBCS score (required)

        Returns:
            Dictionary with all analysis results
        """
        ip_metrics = self.compute_ip_homogeneity(bundles)
        consistency_metrics = self.compute_fingerprint_consistency(bundles)
        collisions = self.detect_collisions(bundles, coherence_scores)

        # Count eligible bundles (≥2 unique URLs)
        eligible_bundles = [b for b in bundles if len(set(b.urls)) >= 2]

        return {
            "ip_homogeneity": ip_metrics.to_dict(),
            "fingerprint_consistency": consistency_metrics.to_dict(),
            "eligible_bundles": len(eligible_bundles),
            "collision_count": len(collisions),
            "collision_rate": (
                len(collisions) / len(eligible_bundles) if eligible_bundles else 0.0
            ),
            "collisions": [
                {
                    "bundle_id": c.bundle.bundle_id,
                    "ip_count": c.ip_count,
                    "subnet_count": c.subnet_count,
                    "ip_homogeneity": c.ip_homogeneity,
                    "mibcs": c.coherence_score,
                    "collision_score": c.collision_score,
                    "reason": c.reason,
                }
                for c in collisions[:10]  # Top 10 collisions
            ],
        }
