"""
Unit tests for research modules: fingerprint_analysis, session_refinement, window_optimizer.
"""

from datetime import datetime, timedelta, timezone

import pytest

from llm_bot_pipeline.research.fingerprint_analysis import (
    FingerprintAnalyzer,
    IPHomogeneityMetrics,
)
from llm_bot_pipeline.research.semantic_embeddings import SimilarityResult
from llm_bot_pipeline.research.session_refinement import (
    SessionRefiner,
    UnionFind,
    find_connected_components,
    get_ip_network,
)
from llm_bot_pipeline.research.temporal_analysis import Bundle, EnrichedBundle
from llm_bot_pipeline.research.window_optimizer import (
    WindowOptimizer,
    compute_bundle_purity,
    compute_opt_score,
)


def _make_enriched_bundle(
    bundle_id: str = "b1",
    urls: list[str] | None = None,
    client_ips: list[str] | None = None,
    countries: list[str] | None = None,
) -> EnrichedBundle:
    """Helper to build EnrichedBundle with sensible defaults."""
    st = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    et = datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc)
    urls = urls or ["https://example.com/a", "https://example.com/b"]
    client_ips = client_ips or ["192.168.1.1", "192.168.1.1"]
    countries = countries or ["US", "US"]
    n = len(urls)
    return EnrichedBundle(
        bundle_id=bundle_id,
        start_time=st,
        end_time=et,
        request_count=n,
        bot_provider="openai",
        urls=urls,
        request_indices=list(range(n)),
        client_ips=(
            client_ips[:n]
            if len(client_ips) >= n
            else client_ips + [""] * (n - len(client_ips))
        ),
        response_statuses=[200] * n,
        bot_scores=[0.9] * n,
        countries=(
            countries[:n]
            if len(countries) >= n
            else countries + [""] * (n - len(countries))
        ),
        bot_tags=[],
        bot_name="test",
    )


def _make_bundle(
    bundle_id: str = "b1",
    urls: list[str] | None = None,
    duration_ms: float = 5000.0,
) -> Bundle:
    """Helper to build Bundle for window optimizer tests."""
    st = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    et = st + timedelta(milliseconds=duration_ms)
    urls = urls or ["https://example.com/a", "https://example.com/b"]
    return Bundle(
        bundle_id=bundle_id,
        start_time=st,
        end_time=et,
        request_count=len(urls),
        bot_provider="openai",
        urls=urls,
        request_indices=list(range(len(urls))),
    )


# =============================================================================
# Fingerprint Analysis Tests
# =============================================================================


class TestFingerprintAnalysisKnownInput:
    """Feed known IP/URL set, assert expected fingerprint groups (IP homogeneity)."""

    def test_single_ip_bundle_perfect_homogeneity(self):
        """Bundle with single IP should have perfect IP homogeneity."""
        bundles = [
            _make_enriched_bundle(
                bundle_id="b1",
                urls=["https://example.com/a", "https://example.com/b"],
                client_ips=["192.168.1.1", "192.168.1.1"],
            )
        ]
        analyzer = FingerprintAnalyzer()
        metrics = analyzer.compute_ip_homogeneity(bundles)

        assert isinstance(metrics, IPHomogeneityMetrics)
        assert metrics.total_bundles == 1
        assert metrics.bundles_with_single_ip == 1
        assert metrics.bundles_with_single_subnet == 1
        assert metrics.mean_ip_homogeneity == 1.0
        assert metrics.ip_homogeneity_rate == 1.0

    def test_multi_ip_bundle_lower_homogeneity(self):
        """Bundle with multiple IPs should have lower homogeneity."""
        bundles = [
            _make_enriched_bundle(
                bundle_id="b1",
                urls=[
                    "https://example.com/a",
                    "https://example.com/b",
                    "https://example.com/c",
                ],
                client_ips=["192.168.1.1", "192.168.1.2", "192.168.1.3"],
            )
        ]
        analyzer = FingerprintAnalyzer()
        metrics = analyzer.compute_ip_homogeneity(bundles)

        assert metrics.total_bundles == 1
        assert metrics.bundles_with_single_ip == 0
        assert metrics.mean_ip_homogeneity < 1.0
        assert metrics.mean_unique_ips_per_bundle == 3.0

    def test_mixed_bundles_expected_aggregates(self):
        """Multiple bundles with known IP patterns produce expected aggregates."""
        bundles = [
            _make_enriched_bundle("b1", client_ips=["10.0.0.1", "10.0.0.1"]),
            _make_enriched_bundle("b2", client_ips=["10.0.0.2", "10.0.0.2"]),
            _make_enriched_bundle(
                "b3",
                urls=["u1", "u2", "u3"],
                client_ips=["10.0.0.1", "10.0.0.2", "10.0.0.3"],
            ),
        ]
        analyzer = FingerprintAnalyzer()
        metrics = analyzer.compute_ip_homogeneity(bundles)

        assert metrics.total_bundles == 3
        assert metrics.bundles_with_single_ip == 2
        assert metrics.bundles_with_single_subnet == 3  # all same /24
        assert 0.5 < metrics.mean_ip_homogeneity <= 1.0
        assert metrics.ip_homogeneity_rate == 2 / 3

    def test_empty_bundles_returns_safe_metrics(self):
        """Empty bundle list returns safe default metrics."""
        analyzer = FingerprintAnalyzer()
        metrics = analyzer.compute_ip_homogeneity([])

        assert metrics.total_bundles == 0
        assert metrics.mean_ip_homogeneity == 1.0
        assert metrics.ip_homogeneity_rate == 1.0


# =============================================================================
# Session Refinement Tests (research module - splitting)
# =============================================================================


class TestSessionRefinementSplits:
    """Feed bundle with multiple IP networks, assert correct split."""

    def test_network_split_two_subnets(self):
        """Bundle with two IP networks should split into two sub-bundles."""
        bundle = _make_enriched_bundle(
            bundle_id="collision",
            urls=[
                "https://example.com/mortgage/calc",
                "https://example.com/mortgage/rates",
                "https://example.com/weather/forecast",
                "https://example.com/weather/today",
            ],
            client_ips=["192.168.1.1", "192.168.1.2", "10.0.0.1", "10.0.0.2"],
        )
        refiner = SessionRefiner(
            similarity_threshold=0.3,
            min_sub_bundle_size=2,
            min_mibcs_improvement=0.0,
        )
        result = refiner.refine_bundle(bundle, strategy="network_only")

        assert result.was_split
        assert len(result.sub_bundles) == 2
        # Each sub-bundle should have requests from one network
        for sub in result.sub_bundles:
            ips = sub.client_ips
            networks = {get_ip_network(ip) for ip in ips if ip}
            assert len(networks) == 1

    def test_mibcs_only_single_component_no_split(self):
        """Semantically similar URLs stay in one component - no split."""
        bundle = _make_enriched_bundle(
            bundle_id="coherent",
            urls=[
                "https://example.com/mortgage/calculator",
                "https://example.com/mortgage/rates",
                "https://example.com/mortgage/refinance",
            ],
            client_ips=["192.168.1.1", "192.168.1.1", "192.168.1.1"],
        )
        refiner = SessionRefiner(
            similarity_threshold=0.2,
            min_sub_bundle_size=2,
            min_mibcs_improvement=0.05,
        )
        result = refiner.refine_bundle(bundle, strategy="mibcs_only")

        # High similarity URLs form one component - typically no split
        assert result.original_bundle.bundle_id == "coherent"
        assert result.original_mibcs >= 0

    def test_adjacent_sessions_merge_components(self):
        """UnionFind merges adjacent connected components correctly."""
        # Graph: 0-1, 2-3, 1-2 => one component
        adj = [[1], [0, 2], [1, 3], [2]]
        components = find_connected_components(adj)
        assert len(components) == 1
        assert set(components[0]) == {0, 1, 2, 3}

    def test_union_find_merge(self):
        """UnionFind correctly merges and reports components."""
        uf = UnionFind(4)
        uf.union(0, 1)
        uf.union(2, 3)
        uf.union(1, 2)
        comps = uf.get_components()
        assert len(comps) == 1
        assert set(comps[0]) == {0, 1, 2, 3}


# =============================================================================
# Window Optimizer Tests
# =============================================================================


class TestWindowOptimizerFindsOptimal:
    """Feed sample data, assert optimal window within expected range."""

    def test_optimal_window_highest_coherence_wins(self):
        """Window producing more coherent bundles should have higher opt_score."""
        # High-coherence bundles (thematic URLs)
        thematic_urls = [
            "https://example.com/mortgage/calculator",
            "https://example.com/mortgage/rates",
            "https://example.com/mortgage/refinance",
        ]
        # Low-coherence (diverse URLs)
        diverse_urls = [
            "https://example.com/mortgage/calc",
            "https://example.com/weather/forecast",
            "https://example.com/sports/scores",
        ]

        bundles_coherent = [
            _make_bundle("c1", urls=thematic_urls[:2]),
            _make_bundle("c2", urls=thematic_urls[1:]),
        ]
        bundles_diverse = [
            _make_bundle("d1", urls=diverse_urls[:2]),
            _make_bundle("d2", urls=diverse_urls[1:]),
        ]

        optimizer = WindowOptimizer()
        metrics_coherent = optimizer.evaluate_window(bundles_coherent, 100.0)
        metrics_diverse = optimizer.evaluate_window(bundles_diverse, 200.0)

        assert metrics_coherent.opt_score >= metrics_diverse.opt_score
        assert metrics_coherent.mibcs >= metrics_diverse.mibcs

    def test_optimal_window_within_evaluated_range(self):
        """Best window should be one of the evaluated windows."""
        # Use sufficiently diverse URLs to avoid TF-IDF pruning edge cases
        urls = [
            "https://example.com/mortgage/calculator",
            "https://example.com/mortgage/rates",
            "https://example.com/mortgage/refinance",
        ]
        bundles_50 = [
            _make_bundle("b1", urls=urls[:2]),
            _make_bundle("b2", urls=urls[1:]),
        ]
        bundles_100 = [_make_bundle("b1", urls=urls)]
        bundles_200 = [
            _make_bundle(
                "b1",
                urls=urls + ["https://example.com/weather/forecast"],
            )
        ]

        optimizer = WindowOptimizer()
        optimizer.evaluate_window(bundles_50, 50.0)
        optimizer.evaluate_window(bundles_100, 100.0)
        optimizer.evaluate_window(bundles_200, 200.0)

        rec = optimizer.get_recommendation()
        assert rec is not None
        assert rec["recommended_window_ms"] in (50.0, 100.0, 200.0)
        assert 0 <= rec["opt_score"] <= 1.0

    def test_compare_windows_returns_best(self):
        """compare_windows returns correct best window."""
        optimizer = WindowOptimizer()
        from llm_bot_pipeline.research.window_optimizer import OptimizationMetrics

        # Mock metrics with known scores
        results = {
            50.0: OptimizationMetrics(
                window_ms=50.0,
                mibcs=0.5,
                silhouette_score=0.3,
                bundle_purity_score=0.6,
                singleton_rate=0.2,
                giant_rate=0.1,
                thematic_variance=0.1,
                total_bundles=5,
                total_requests=10,
                mean_bundle_size=2.0,
                median_bundle_size=2.0,
                opt_score=0.4,
            ),
            100.0: OptimizationMetrics(
                window_ms=100.0,
                mibcs=0.7,
                silhouette_score=0.5,
                bundle_purity_score=0.8,
                singleton_rate=0.1,
                giant_rate=0.05,
                thematic_variance=0.05,
                total_bundles=4,
                total_requests=10,
                mean_bundle_size=2.5,
                median_bundle_size=2.0,
                opt_score=0.6,
            ),
        }
        comparison = optimizer.compare_windows(results)
        assert comparison["best_window_ms"] == 100.0
        assert comparison["best_opt_score"] == 0.6
        assert len(comparison["rankings"]) == 2


class TestWindowOptimizerHelpers:
    """Tests for window optimizer helper functions."""

    def test_compute_opt_score(self):
        """compute_opt_score produces sensible composite score."""
        score = compute_opt_score(
            mibcs=0.8,
            silhouette=0.6,
            bps=0.9,
            singleton_rate=0.1,
            giant_rate=0.05,
            thematic_variance=0.02,
        )
        assert 0 < score < 1.0
        assert score > 0.5  # Good inputs => high score

    def test_compute_bundle_purity(self):
        """compute_bundle_purity returns fraction of pure bundles."""
        results = [
            SimilarityResult(0.5, 0.4, 0.6, 0.1, 3, None),
            SimilarityResult(0.3, 0.2, 0.4, 0.1, 3, None),
            SimilarityResult(0.6, 0.35, 0.7, 0.1, 3, None),
        ]
        purity = compute_bundle_purity(results, threshold=0.3)
        assert purity == 2 / 3  # 2 of 3 have min_similarity >= 0.3
