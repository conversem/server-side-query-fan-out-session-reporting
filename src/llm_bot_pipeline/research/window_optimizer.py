"""
Window optimization for query fan-out session bundling.

Provides metrics computation and optimization for selecting
the optimal time window for bundling LLM bot requests.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from .semantic_embeddings import (
    BundleSimilarityAnalyzer,
    SimilarityResult,
    URLEmbedder,
    get_confidence_level,
)
from .temporal_analysis import Bundle, compute_bundle_statistics

logger = logging.getLogger(__name__)

# Try to import silhouette_score from sklearn
try:
    from sklearn.metrics import silhouette_score as sklearn_silhouette_score

    SKLEARN_METRICS_AVAILABLE = True
except ImportError:
    SKLEARN_METRICS_AVAILABLE = False
    logger.debug("sklearn.metrics not available for silhouette score")


@dataclass
class BundleMetrics:
    """Metrics for a single bundle."""

    bundle_id: str
    request_count: int
    unique_urls: int
    mean_similarity: float
    min_similarity: float
    max_similarity: float
    confidence_level: str
    duration_ms: float


@dataclass
class OptimizationMetrics:
    """Complete optimization metrics for a window configuration."""

    window_ms: float

    # Primary metrics
    mibcs: float  # Mean Intra-Bundle Cosine Similarity
    silhouette_score: Optional[float]  # Cluster quality score
    bundle_purity_score: float  # Fraction of bundles meeting threshold

    # Secondary metrics
    singleton_rate: float  # Rate of single-URL bundles (1 unique URL)
    giant_rate: float  # Rate of bundles with >10 unique URLs
    thematic_variance: float  # Variance in similarity scores

    # Bundle statistics
    total_bundles: int
    total_requests: int
    mean_bundle_size: float
    median_bundle_size: float

    # Composite score
    opt_score: float

    # Per-bundle metrics (optional)
    bundle_metrics: list[BundleMetrics] = field(default_factory=list)


@dataclass
class OptimizationWeights:
    """Weights for the composite optimization function."""

    alpha: float = 0.30  # MIBCS weight
    beta: float = 0.25  # Silhouette weight
    gamma: float = 0.25  # Bundle Purity weight
    delta: float = 0.10  # Singleton rate penalty
    epsilon: float = 0.05  # Giant rate penalty
    zeta: float = 0.05  # Thematic variance penalty


def compute_silhouette_score(
    bundles: list[Bundle],
    embedder: URLEmbedder,
) -> Optional[float]:
    """
    Compute silhouette score for bundle clustering.

    Args:
        bundles: List of Bundle objects
        embedder: URLEmbedder for generating embeddings

    Returns:
        Silhouette score in range [-1, 1], or None if cannot compute
    """
    if not SKLEARN_METRICS_AVAILABLE:
        return None

    # Filter bundles with multiple URLs
    valid_bundles = [b for b in bundles if len(b.urls) >= 1]

    if len(valid_bundles) < 2:
        return None

    # Collect all URLs and their bundle labels
    all_urls = []
    labels = []
    for i, bundle in enumerate(valid_bundles):
        for url in bundle.urls:
            all_urls.append(url)
            labels.append(i)

    if len(set(labels)) < 2:
        return None

    # Generate embeddings
    try:
        embeddings = embedder.embed(all_urls)
        score = sklearn_silhouette_score(embeddings, labels, metric="cosine")
        return float(score)
    except Exception as e:
        logger.warning(f"Failed to compute silhouette score: {e}")
        return None


def compute_bundle_purity(
    similarity_results: list[SimilarityResult],
    threshold: float = 0.3,
) -> float:
    """
    Compute bundle purity score.

    Args:
        similarity_results: List of SimilarityResult for each bundle
        threshold: Minimum similarity threshold for "pure" bundle

    Returns:
        Fraction of bundles meeting the threshold
    """
    if not similarity_results:
        return 0.0

    pure_count = sum(1 for r in similarity_results if r.min_similarity >= threshold)
    return pure_count / len(similarity_results)


def compute_opt_score(
    mibcs: float,
    silhouette: Optional[float],
    bps: float,
    singleton_rate: float,
    giant_rate: float,
    thematic_variance: float,
    weights: Optional[OptimizationWeights] = None,
) -> float:
    """
    Compute composite optimization score.

    OptScore = α·MIBCS + β·S + γ·BPS - δ·SR - ε·GBR - ζ·TV

    Args:
        mibcs: Mean Intra-Bundle Cosine Similarity
        silhouette: Silhouette score (optional)
        bps: Bundle Purity Score
        singleton_rate: Rate of singleton bundles
        giant_rate: Rate of giant bundles (>10 requests)
        thematic_variance: Variance in similarity scores
        weights: Optimization weights

    Returns:
        Composite optimization score
    """
    if weights is None:
        weights = OptimizationWeights()

    # Use 0 for silhouette if not available
    s = silhouette if silhouette is not None else 0.0

    score = (
        weights.alpha * mibcs
        + weights.beta * s
        + weights.gamma * bps
        - weights.delta * singleton_rate
        - weights.epsilon * giant_rate
        - weights.zeta * thematic_variance
    )

    return score


class WindowOptimizer:
    """
    Optimize time window for query fan-out session bundling.

    Evaluates multiple candidate windows and selects the optimal
    one based on composite optimization score.
    """

    def __init__(
        self,
        embedder: Optional[URLEmbedder] = None,
        weights: Optional[OptimizationWeights] = None,
        purity_threshold: float = 0.3,
    ):
        """
        Initialize window optimizer.

        Args:
            embedder: URLEmbedder for semantic analysis
            weights: Optimization weights
            purity_threshold: Threshold for bundle purity score
        """
        self.embedder = embedder or URLEmbedder(method="tfidf")
        self.weights = weights or OptimizationWeights()
        self.purity_threshold = purity_threshold
        self._results: dict[float, OptimizationMetrics] = {}

    def evaluate_window(
        self,
        bundles: list[Bundle],
        window_ms: float,
        include_bundle_metrics: bool = False,
    ) -> OptimizationMetrics:
        """
        Evaluate a single window configuration.

        Args:
            bundles: List of Bundle objects created with this window
            window_ms: Window size in milliseconds
            include_bundle_metrics: Whether to include per-bundle metrics

        Returns:
            OptimizationMetrics for this window
        """
        # Fit embedder on all URLs
        all_urls = []
        for bundle in bundles:
            all_urls.extend(bundle.urls)

        if all_urls:
            self.embedder.fit(all_urls)

        # Compute bundle statistics
        bundle_stats = compute_bundle_statistics(bundles)

        # Compute similarity for each bundle
        analyzer = BundleSimilarityAnalyzer(embedder=self.embedder)
        similarity_results = analyzer.analyze_bundles(bundles)

        # Compute primary metrics
        if similarity_results:
            mean_sims = [r.mean_similarity for r in similarity_results]
            mibcs = float(np.mean(mean_sims))
            thematic_variance = float(np.var(mean_sims))
        else:
            mibcs = 0.0
            thematic_variance = 0.0

        silhouette = compute_silhouette_score(bundles, self.embedder)
        bps = compute_bundle_purity(similarity_results, self.purity_threshold)

        # Compute composite score
        opt_score = compute_opt_score(
            mibcs=mibcs,
            silhouette=silhouette,
            bps=bps,
            singleton_rate=bundle_stats["singleton_rate"],
            giant_rate=bundle_stats["giant_rate"],
            thematic_variance=thematic_variance,
            weights=self.weights,
        )

        # Build per-bundle metrics if requested
        bundle_metrics = []
        if include_bundle_metrics:
            for bundle, sim_result in zip(bundles, similarity_results):
                confidence = get_confidence_level(
                    sim_result.mean_similarity,
                    sim_result.min_similarity,
                )
                bundle_metrics.append(
                    BundleMetrics(
                        bundle_id=bundle.bundle_id,
                        request_count=bundle.request_count,
                        unique_urls=len(set(bundle.urls)),
                        mean_similarity=sim_result.mean_similarity,
                        min_similarity=sim_result.min_similarity,
                        max_similarity=sim_result.max_similarity,
                        confidence_level=confidence,
                        duration_ms=bundle.duration_ms,
                    )
                )

        metrics = OptimizationMetrics(
            window_ms=window_ms,
            mibcs=mibcs,
            silhouette_score=silhouette,
            bundle_purity_score=bps,
            singleton_rate=bundle_stats["singleton_rate"],
            giant_rate=bundle_stats["giant_rate"],
            thematic_variance=thematic_variance,
            total_bundles=bundle_stats["total_bundles"],
            total_requests=bundle_stats["total_requests"],
            mean_bundle_size=bundle_stats["mean_bundle_size"],
            median_bundle_size=bundle_stats["median_bundle_size"],
            opt_score=opt_score,
            bundle_metrics=bundle_metrics,
        )

        self._results[window_ms] = metrics
        return metrics

    def compare_windows(
        self,
        window_results: dict[float, OptimizationMetrics],
    ) -> dict:
        """
        Compare multiple window configurations.

        Args:
            window_results: Dictionary mapping window_ms to OptimizationMetrics

        Returns:
            Comparison summary with rankings
        """
        if not window_results:
            return {}

        # Sort by OptScore
        sorted_windows = sorted(
            window_results.items(),
            key=lambda x: x[1].opt_score,
            reverse=True,
        )

        # Find best window
        best_window, best_metrics = sorted_windows[0]

        return {
            "best_window_ms": best_window,
            "best_opt_score": best_metrics.opt_score,
            "rankings": [
                {
                    "rank": i + 1,
                    "window_ms": w,
                    "opt_score": m.opt_score,
                    "mibcs": m.mibcs,
                    "silhouette": m.silhouette_score,
                    "bps": m.bundle_purity_score,
                    "singleton_rate": m.singleton_rate,
                    "giant_rate": m.giant_rate,
                }
                for i, (w, m) in enumerate(sorted_windows)
            ],
            "window_count": len(window_results),
        }

    def get_recommendation(self) -> Optional[dict]:
        """
        Get recommendation based on evaluated windows.

        Returns:
            Recommendation dictionary or None if no windows evaluated
        """
        if not self._results:
            return None

        comparison = self.compare_windows(self._results)
        best_window = comparison["best_window_ms"]
        best_metrics = self._results[best_window]

        # Determine confidence in recommendation
        if len(self._results) >= 3:
            scores = [m.opt_score for m in self._results.values()]
            score_range = max(scores) - min(scores)
            if score_range > 0.1:
                confidence = "high"
            elif score_range > 0.05:
                confidence = "medium"
            else:
                confidence = "low"
        else:
            confidence = "low"

        return {
            "recommended_window_ms": best_window,
            "opt_score": best_metrics.opt_score,
            "mibcs": best_metrics.mibcs,
            "silhouette_score": best_metrics.silhouette_score,
            "bundle_purity_score": best_metrics.bundle_purity_score,
            "recommendation_confidence": confidence,
            "total_bundles": best_metrics.total_bundles,
            "mean_bundle_size": best_metrics.mean_bundle_size,
            "singleton_rate": best_metrics.singleton_rate,
            "giant_rate": best_metrics.giant_rate,
        }

    def to_dataframe(self):
        """
        Export results to pandas DataFrame.

        Returns:
            DataFrame with window comparison data
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas required for DataFrame export")

        rows = []
        for window_ms, metrics in sorted(self._results.items()):
            rows.append(
                {
                    "window_ms": window_ms,
                    "opt_score": metrics.opt_score,
                    "mibcs": metrics.mibcs,
                    "silhouette_score": metrics.silhouette_score,
                    "bundle_purity_score": metrics.bundle_purity_score,
                    "singleton_rate": metrics.singleton_rate,
                    "giant_rate": metrics.giant_rate,
                    "thematic_variance": metrics.thematic_variance,
                    "total_bundles": metrics.total_bundles,
                    "total_requests": metrics.total_requests,
                    "mean_bundle_size": metrics.mean_bundle_size,
                    "median_bundle_size": metrics.median_bundle_size,
                }
            )

        return pd.DataFrame(rows)
