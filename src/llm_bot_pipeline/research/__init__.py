"""
Research module for query fan-out bundling analysis.

This module provides tools for:
- Temporal analysis of inter-request time deltas
- Semantic embedding of URLs for similarity analysis
- Window optimization to determine optimal bundling thresholds
- Experiment orchestration for systematic evaluation

See docs/prds/query-fanout-bundling-PRD.md for full methodology.
"""

from .experiment_runner import ExperimentConfig, ExperimentResult, ExperimentRunner
from .semantic_embeddings import (
    CONFIDENCE_THRESHOLDS,
    BundleSimilarityAnalyzer,
    SimilarityResult,
    URLEmbedder,
    compute_bundle_similarity,
    compute_cosine_similarity,
    compute_pairwise_similarity,
    get_confidence_level,
    tokenize_url,
    tokenize_urls,
)
from .temporal_analysis import (
    Bundle,
    DeltaStats,
    TemporalAnalyzer,
    compute_bundle_statistics,
    compute_inter_request_deltas,
    create_temporal_bundles,
    find_natural_gaps,
)
from .window_optimizer import (
    BundleMetrics,
    OptimizationMetrics,
    OptimizationWeights,
    WindowOptimizer,
)

__all__ = [
    # Temporal analysis
    "TemporalAnalyzer",
    "Bundle",
    "DeltaStats",
    "compute_inter_request_deltas",
    "compute_bundle_statistics",
    "create_temporal_bundles",
    "find_natural_gaps",
    # Semantic embeddings
    "URLEmbedder",
    "BundleSimilarityAnalyzer",
    "SimilarityResult",
    "tokenize_url",
    "tokenize_urls",
    "compute_cosine_similarity",
    "compute_pairwise_similarity",
    "compute_bundle_similarity",
    # Confidence classification
    "CONFIDENCE_THRESHOLDS",
    "get_confidence_level",
    # Window optimization
    "WindowOptimizer",
    "OptimizationMetrics",
    "OptimizationWeights",
    "BundleMetrics",
    # Experiment runner
    "ExperimentRunner",
    "ExperimentConfig",
    "ExperimentResult",
]
