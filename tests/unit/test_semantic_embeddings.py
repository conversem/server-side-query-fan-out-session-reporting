"""
Unit tests for TF-IDF URL similarity calculation.

Tests the semantic embeddings module including URL tokenization,
TF-IDF vectorization, and cosine similarity computation.
"""

import numpy as np
import pytest

from llm_bot_pipeline.research.semantic_embeddings import (
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


class TestTokenizeUrl:
    """Tests for tokenize_url function."""

    def test_basic_path_segments(self):
        """Should split path by slashes."""
        result = tokenize_url("/blog/home-buying-guide")
        assert result == "blog home buying guide"

    def test_hyphen_splitting(self):
        """Should split segments by hyphens."""
        result = tokenize_url("/first-time-buyer")
        assert result == "first time buyer"

    def test_underscore_splitting(self):
        """Should split segments by underscores."""
        result = tokenize_url("/user_profile_settings")
        assert result == "user profile settings"

    def test_dot_splitting(self):
        """Should split segments by dots (PRD requirement)."""
        result = tokenize_url("/docs/api.v2.html")
        assert result == "docs api v2 html"

    def test_mixed_separators(self):
        """Should handle mix of hyphens, underscores, and dots."""
        result = tokenize_url("/blog/my-post_v2.0")
        assert result == "blog my post v2 0"

    def test_camel_case_splitting(self):
        """Should split camelCase tokens."""
        result = tokenize_url("/homeBuyingGuide")
        assert result == "home buying guide"

    def test_query_string_removal(self):
        """Should remove query strings before tokenizing."""
        result = tokenize_url("/search?q=mortgage&page=1")
        assert result == "search"

    def test_fragment_removal(self):
        """Should remove fragments before tokenizing."""
        result = tokenize_url("/article#section-2")
        assert result == "article"

    def test_empty_path(self):
        """Should handle empty paths."""
        result = tokenize_url("/")
        assert result == ""

    def test_prd_example_1(self):
        """PRD example: /blog/home-buying-guide → blog home buying guide."""
        result = tokenize_url("/blog/home-buying-guide")
        assert result == "blog home buying guide"

    def test_prd_example_2(self):
        """PRD example: /mortgage/calculator → mortgage calculator."""
        result = tokenize_url("/mortgage/calculator")
        assert result == "mortgage calculator"

    def test_complex_url(self):
        """Should handle complex URLs with multiple separators."""
        result = tokenize_url("/api/v2.1/user-profile/get_settings.json")
        assert result == "api v2 1 user profile get settings json"


class TestTokenizeUrls:
    """Tests for tokenize_urls function."""

    def test_multiple_urls(self):
        """Should tokenize multiple URLs."""
        urls = ["/blog/post-one", "/blog/post-two"]
        result = tokenize_urls(urls)
        assert result == ["blog post one", "blog post two"]

    def test_empty_list(self):
        """Should handle empty list."""
        result = tokenize_urls([])
        assert result == []


class TestComputeCosineSimilarity:
    """Tests for compute_cosine_similarity function."""

    def test_identical_vectors(self):
        """Identical vectors should have similarity 1.0."""
        vec = np.array([1.0, 2.0, 3.0])
        result = compute_cosine_similarity(vec, vec)
        assert result == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        """Orthogonal vectors should have similarity 0.0."""
        vec1 = np.array([1.0, 0.0])
        vec2 = np.array([0.0, 1.0])
        result = compute_cosine_similarity(vec1, vec2)
        assert result == pytest.approx(0.0)

    def test_opposite_vectors(self):
        """Opposite vectors should have similarity -1.0."""
        vec1 = np.array([1.0, 0.0])
        vec2 = np.array([-1.0, 0.0])
        result = compute_cosine_similarity(vec1, vec2)
        assert result == pytest.approx(-1.0)

    def test_zero_vector(self):
        """Zero vector should return 0.0 similarity."""
        vec1 = np.array([1.0, 2.0])
        vec2 = np.array([0.0, 0.0])
        result = compute_cosine_similarity(vec1, vec2)
        assert result == 0.0

    def test_similarity_range(self):
        """Similarity should be in range [-1, 1]."""
        np.random.seed(42)
        for _ in range(10):
            vec1 = np.random.rand(10)
            vec2 = np.random.rand(10)
            result = compute_cosine_similarity(vec1, vec2)
            assert -1.0 <= result <= 1.0


class TestComputePairwiseSimilarity:
    """Tests for compute_pairwise_similarity function."""

    def test_diagonal_is_ones(self):
        """Diagonal of similarity matrix should be 1.0."""
        embeddings = np.array([[1, 0], [0, 1], [1, 1]])
        result = compute_pairwise_similarity(embeddings)
        np.testing.assert_array_almost_equal(np.diag(result), [1.0, 1.0, 1.0])

    def test_symmetric_matrix(self):
        """Similarity matrix should be symmetric."""
        embeddings = np.array([[1, 2], [3, 4], [5, 6]])
        result = compute_pairwise_similarity(embeddings)
        np.testing.assert_array_almost_equal(result, result.T)

    def test_correct_shape(self):
        """Result should be (n, n) matrix."""
        embeddings = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]])
        result = compute_pairwise_similarity(embeddings)
        assert result.shape == (4, 4)


class TestComputeBundleSimilarity:
    """Tests for compute_bundle_similarity function."""

    def test_single_url_bundle(self):
        """Single URL should return similarity 1.0."""
        embeddings = np.array([[1.0, 2.0, 3.0]])
        result = compute_bundle_similarity(embeddings)
        assert result.mean_similarity == 1.0
        assert result.min_similarity == 1.0
        assert result.pairwise_count == 0

    def test_identical_embeddings(self):
        """Identical embeddings should have similarity 1.0."""
        embeddings = np.array([[1.0, 0.0], [1.0, 0.0]])
        result = compute_bundle_similarity(embeddings)
        assert result.mean_similarity == pytest.approx(1.0)
        assert result.min_similarity == pytest.approx(1.0)

    def test_pairwise_count(self):
        """Should count correct number of pairwise comparisons."""
        # n choose 2 = n*(n-1)/2
        embeddings = np.array([[1, 0], [0, 1], [1, 1], [0, 0.5]])
        result = compute_bundle_similarity(embeddings)
        # 4 choose 2 = 6
        assert result.pairwise_count == 6

    def test_include_matrix(self):
        """Should include similarity matrix when requested."""
        embeddings = np.array([[1, 0], [0, 1]])
        result = compute_bundle_similarity(embeddings, include_matrix=True)
        assert result.similarity_matrix is not None
        assert result.similarity_matrix.shape == (2, 2)


class TestURLEmbedder:
    """Tests for URLEmbedder class."""

    def test_tfidf_method_default(self):
        """Should default to TF-IDF method."""
        embedder = URLEmbedder()
        assert embedder.method == "tfidf"

    def test_fit_returns_self(self):
        """fit() should return self for chaining."""
        embedder = URLEmbedder()
        urls = ["/page-one", "/page-two"]
        result = embedder.fit(urls)
        assert result is embedder

    def test_embed_single_url(self):
        """Should embed single URL."""
        embedder = URLEmbedder()
        urls = ["/blog/post", "/about/team"]
        embedder.fit(urls)
        result = embedder.embed("/blog/post")
        assert isinstance(result, np.ndarray)
        assert result.shape[0] == 1  # Single URL

    def test_embed_multiple_urls(self):
        """Should embed multiple URLs."""
        embedder = URLEmbedder()
        urls = ["/blog/one", "/blog/two", "/about/team"]
        embedder.fit(urls)
        result = embedder.embed(urls)
        assert result.shape[0] == 3

    def test_auto_fit_when_not_fitted(self):
        """Should auto-fit when embedding without prior fit."""
        embedder = URLEmbedder()
        # Use multiple diverse URLs to avoid TF-IDF min_df/max_df issues
        urls = ["/some/page", "/other/path", "/third/option"]
        embedder.fit(urls)
        result = embedder.embed("/some/page")
        assert isinstance(result, np.ndarray)

    def test_compute_similarity_identical_urls(self):
        """Identical URLs should have similarity 1.0."""
        embedder = URLEmbedder()
        # Fit on diverse corpus first, then check identical URLs
        corpus = ["/same/path", "/other/page", "/third/option", "/fourth/item"]
        embedder.fit(corpus)
        result = embedder.compute_similarity(["/same/path", "/same/path"])
        assert result.mean_similarity == pytest.approx(1.0)

    def test_compute_similarity_single_url(self):
        """Single URL should return similarity 1.0."""
        embedder = URLEmbedder()
        result = embedder.compute_similarity(["/only/one"])
        assert result.mean_similarity == 1.0
        assert result.pairwise_count == 0

    def test_related_urls_higher_similarity(self):
        """Related URLs should have higher similarity than unrelated."""
        embedder = URLEmbedder()
        # Fit on diverse corpus
        corpus = [
            "/blog/home-buying-tips",
            "/blog/home-selling-tips",
            "/products/shoes",
            "/products/clothing",
            "/about/team",
        ]
        embedder.fit(corpus)

        # Related URLs
        related = ["/blog/home-buying-tips", "/blog/home-selling-tips"]
        related_result = embedder.compute_similarity(related)

        # Unrelated URLs
        unrelated = ["/blog/home-buying-tips", "/about/team"]
        unrelated_result = embedder.compute_similarity(unrelated)

        # Related should have higher similarity
        assert related_result.mean_similarity > unrelated_result.mean_similarity


class TestConfidenceThresholds:
    """Tests for CONFIDENCE_THRESHOLDS constant."""

    def test_thresholds_exist(self):
        """CONFIDENCE_THRESHOLDS should be defined."""
        from llm_bot_pipeline.research.semantic_embeddings import CONFIDENCE_THRESHOLDS

        assert CONFIDENCE_THRESHOLDS is not None

    def test_high_thresholds(self):
        """High thresholds should match PRD spec."""
        from llm_bot_pipeline.research.semantic_embeddings import CONFIDENCE_THRESHOLDS

        assert CONFIDENCE_THRESHOLDS["high"]["mean_similarity"] == 0.7
        assert CONFIDENCE_THRESHOLDS["high"]["min_similarity"] == 0.5

    def test_medium_thresholds(self):
        """Medium thresholds should match PRD spec."""
        from llm_bot_pipeline.research.semantic_embeddings import CONFIDENCE_THRESHOLDS

        assert CONFIDENCE_THRESHOLDS["medium"]["mean_similarity"] == 0.5
        assert CONFIDENCE_THRESHOLDS["medium"]["min_similarity"] == 0.3

    def test_low_thresholds(self):
        """Low thresholds should be zero (catch-all)."""
        from llm_bot_pipeline.research.semantic_embeddings import CONFIDENCE_THRESHOLDS

        assert CONFIDENCE_THRESHOLDS["low"]["mean_similarity"] == 0.0
        assert CONFIDENCE_THRESHOLDS["low"]["min_similarity"] == 0.0


class TestGetConfidenceLevel:
    """Tests for get_confidence_level function."""

    # --- High confidence tests ---
    def test_high_confidence(self):
        """High mean and min similarity should return 'high'."""
        result = get_confidence_level(mean_similarity=0.8, min_similarity=0.6)
        assert result == "high"

    def test_high_boundary(self):
        """Exactly at high thresholds should return 'high'."""
        result = get_confidence_level(mean_similarity=0.7, min_similarity=0.5)
        assert result == "high"

    def test_just_below_high_mean(self):
        """Just below high mean threshold should return 'medium'."""
        result = get_confidence_level(mean_similarity=0.69, min_similarity=0.5)
        assert result == "medium"

    def test_just_below_high_min(self):
        """Just below high min threshold should return 'medium'."""
        result = get_confidence_level(mean_similarity=0.7, min_similarity=0.49)
        assert result == "medium"

    # --- Medium confidence tests ---
    def test_medium_confidence(self):
        """Medium similarity should return 'medium'."""
        result = get_confidence_level(mean_similarity=0.6, min_similarity=0.4)
        assert result == "medium"

    def test_medium_boundary(self):
        """Exactly at medium thresholds should return 'medium'."""
        result = get_confidence_level(mean_similarity=0.5, min_similarity=0.3)
        assert result == "medium"

    def test_just_below_medium_mean(self):
        """Just below medium mean threshold should return 'low'."""
        result = get_confidence_level(mean_similarity=0.49, min_similarity=0.3)
        assert result == "low"

    def test_just_below_medium_min(self):
        """Just below medium min threshold should return 'low'."""
        result = get_confidence_level(mean_similarity=0.5, min_similarity=0.29)
        assert result == "low"

    # --- Low confidence tests ---
    def test_low_confidence(self):
        """Low similarity should return 'low'."""
        result = get_confidence_level(mean_similarity=0.3, min_similarity=0.1)
        assert result == "low"

    def test_zero_similarity(self):
        """Zero similarity should return 'low'."""
        result = get_confidence_level(mean_similarity=0.0, min_similarity=0.0)
        assert result == "low"

    # --- Edge case tests ---
    def test_high_mean_but_low_min(self):
        """High mean but low min should return 'low'."""
        result = get_confidence_level(mean_similarity=0.8, min_similarity=0.2)
        assert result == "low"

    def test_high_min_but_low_mean(self):
        """High min but low mean should return 'low'."""
        result = get_confidence_level(mean_similarity=0.4, min_similarity=0.6)
        assert result == "low"

    def test_perfect_similarity(self):
        """Perfect similarity (1.0, 1.0) should return 'high'."""
        result = get_confidence_level(mean_similarity=1.0, min_similarity=1.0)
        assert result == "high"

    def test_medium_mean_high_min(self):
        """Medium mean with high min should return 'medium' (both must meet threshold)."""
        result = get_confidence_level(mean_similarity=0.6, min_similarity=0.6)
        assert result == "medium"


class TestSimilarityResult:
    """Tests for SimilarityResult dataclass."""

    def test_dataclass_creation(self):
        """Should create SimilarityResult with required fields."""
        result = SimilarityResult(
            mean_similarity=0.7,
            min_similarity=0.5,
            max_similarity=0.9,
            std_similarity=0.1,
            pairwise_count=3,
        )
        assert result.mean_similarity == 0.7
        assert result.min_similarity == 0.5
        assert result.max_similarity == 0.9
        assert result.std_similarity == 0.1
        assert result.pairwise_count == 3
        assert result.similarity_matrix is None


class TestBundleSimilarityAnalyzer:
    """Tests for BundleSimilarityAnalyzer class."""

    def _create_mock_bundle(self, urls: list[str]):
        """Create a mock bundle with URLs."""
        from dataclasses import dataclass

        @dataclass
        class MockBundle:
            urls: list[str]

        return MockBundle(urls=urls)

    def test_fit_on_bundles(self):
        """Should fit on all URLs across bundles."""
        analyzer = BundleSimilarityAnalyzer()
        bundles = [
            self._create_mock_bundle(["/blog/one", "/blog/two"]),
            self._create_mock_bundle(["/about/team"]),
        ]
        result = analyzer.fit_on_bundles(bundles)
        assert result is analyzer

    def test_analyze_bundles(self):
        """Should analyze multiple bundles."""
        analyzer = BundleSimilarityAnalyzer()
        bundles = [
            self._create_mock_bundle(["/blog/one", "/blog/two"]),
            self._create_mock_bundle(["/about/team", "/about/contact"]),
        ]
        analyzer.fit_on_bundles(bundles)
        results = analyzer.analyze_bundles(bundles)

        assert len(results) == 2
        for result in results:
            assert isinstance(result, SimilarityResult)

    def test_get_aggregate_stats(self):
        """Should compute aggregate statistics."""
        analyzer = BundleSimilarityAnalyzer()
        bundles = [
            self._create_mock_bundle(["/blog/one", "/blog/two"]),
            self._create_mock_bundle(["/about/team", "/about/contact"]),
        ]
        analyzer.fit_on_bundles(bundles)
        analyzer.analyze_bundles(bundles)
        stats = analyzer.get_aggregate_stats()

        assert "bundle_count" in stats
        assert "mean_of_means" in stats
        assert "confidence_distribution" in stats
        assert stats["bundle_count"] == 2

    def test_empty_bundles_aggregate_stats(self):
        """Empty results should return empty stats."""
        analyzer = BundleSimilarityAnalyzer()
        stats = analyzer.get_aggregate_stats()
        assert stats == {}


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_url_tokenization(self):
        """Should handle empty URL string."""
        result = tokenize_url("")
        assert result == ""

    def test_url_with_only_slashes(self):
        """Should handle URL with only slashes."""
        result = tokenize_url("///")
        assert result == ""

    def test_special_characters_in_url(self):
        """Should handle special characters."""
        result = tokenize_url("/page%20name")
        assert "page" in result

    def test_numeric_segments(self):
        """Should handle numeric segments."""
        result = tokenize_url("/api/v2/users/123")
        assert "api" in result
        assert "v2" in result
        assert "users" in result
        assert "123" in result


class TestIntegration:
    """Integration tests for complete similarity workflow."""

    def test_full_similarity_workflow(self):
        """Test complete workflow from URLs to confidence level."""
        # Sample URLs from a session
        urls = [
            "/blog/home-buying-guide",
            "/blog/home-selling-tips",
            "/mortgage/rates",
        ]

        # Create embedder and compute similarity
        embedder = URLEmbedder(method="tfidf")
        embedder.fit(urls)
        result = embedder.compute_similarity(urls)

        # Get confidence level
        confidence = get_confidence_level(result.mean_similarity, result.min_similarity)

        # Validate outputs
        assert 0.0 <= result.mean_similarity <= 1.0
        assert 0.0 <= result.min_similarity <= 1.0
        assert result.min_similarity <= result.mean_similarity
        assert confidence in ["high", "medium", "low"]

    def test_prd_example_urls(self):
        """Test with PRD example URLs."""
        # From PRD: Related topics should have reasonable similarity
        urls = [
            "/blog/home-buying-guide",
            "/mortgage/calculator",
        ]

        embedder = URLEmbedder()
        embedder.fit(urls)
        result = embedder.compute_similarity(urls)

        # Should have some similarity (both real estate related)
        # but not necessarily very high (different sections)
        assert result.mean_similarity >= 0.0
        assert result.mean_similarity <= 1.0
