"""
Research module for query fan-out bundling analysis.

This module provides tools for:
- Temporal analysis of inter-request time deltas
- Semantic embedding of URLs for similarity analysis
- Window optimization to determine optimal bundling thresholds
- Fingerprint analysis for IP homogeneity and collision detection
- Experiment orchestration for systematic evaluation

See docs/query-fanout-sessions.md for full methodology.
"""

from .experiment_runner import ExperimentConfig, ExperimentResult, ExperimentRunner
from .fingerprint_analysis import (
    BundlingComparisonMetrics,
    CollisionCandidate,
    FingerprintAnalyzer,
    FingerprintConsistencyMetrics,
    IPHomogeneityMetrics,
)
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
from .session_refinement import (
    RefinementResult,
    SessionRefiner,
    SplitCandidate,
    UnionFind,
    build_similarity_graph,
    find_connected_components,
)
from .temporal_analysis import (
    Bundle,
    DeltaStats,
    EnrichedBundle,
    TemporalAnalyzer,
    compute_bundle_statistics,
    compute_inter_request_deltas,
    compute_ip_homogeneity,
    create_temporal_bundles,
    find_natural_gaps,
    get_subnet_24,
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
    "EnrichedBundle",
    "DeltaStats",
    "compute_inter_request_deltas",
    "compute_bundle_statistics",
    "create_temporal_bundles",
    "find_natural_gaps",
    "compute_ip_homogeneity",
    "get_subnet_24",
    # Fingerprint analysis
    "FingerprintAnalyzer",
    "IPHomogeneityMetrics",
    "FingerprintConsistencyMetrics",
    "CollisionCandidate",
    "BundlingComparisonMetrics",
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
    # Session refinement
    "SessionRefiner",
    "RefinementResult",
    "SplitCandidate",
    "UnionFind",
    "build_similarity_graph",
    "find_connected_components",
    # Experiment runner
    "ExperimentRunner",
    "ExperimentConfig",
    "ExperimentResult",
]
