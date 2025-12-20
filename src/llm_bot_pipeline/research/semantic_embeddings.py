"""
Semantic embeddings for URL similarity analysis.

Provides tools for generating URL embeddings and computing
cosine similarity to validate thematic coherence of session bundles.
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional, Union

import numpy as np

logger = logging.getLogger(__name__)

# Optional imports for advanced embeddings
try:
    from sentence_transformers import SentenceTransformer

    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    logger.debug("sentence-transformers not installed, using TF-IDF fallback")

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity as sklearn_cosine_similarity

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn not installed, limited functionality")


@dataclass
class SimilarityResult:
    """Result of similarity computation for a bundle."""

    mean_similarity: float
    min_similarity: float
    max_similarity: float
    std_similarity: float
    pairwise_count: int
    similarity_matrix: Optional[np.ndarray] = None


def tokenize_url(url: str) -> str:
    """
    Tokenize a URL path into space-separated tokens.

    Splits URL path by '/', '-', '_', '.' characters as specified in PRD.
    Also handles camelCase splitting for additional token extraction.

    Args:
        url: URL path string

    Returns:
        Space-separated tokens suitable for text embedding

    Examples:
        >>> tokenize_url("/blog/home-buying-guide")
        'blog home buying guide'
        >>> tokenize_url("/mortgage/calculator")
        'mortgage calculator'
        >>> tokenize_url("/docs/api.v2.html")
        'docs api v2 html'
    """
    # Remove query string and fragment
    path = url.split("?")[0].split("#")[0]

    # Split by slashes
    segments = path.split("/")

    tokens = []
    for segment in segments:
        if not segment:
            continue

        # Split by hyphens, underscores, and dots (per PRD spec)
        parts = re.split(r"[-_.]", segment)

        for part in parts:
            if not part:
                continue

            # Split camelCase
            camel_split = re.sub(r"([a-z])([A-Z])", r"\1 \2", part)
            tokens.extend(camel_split.lower().split())

    return " ".join(tokens)


def tokenize_urls(urls: list[str]) -> list[str]:
    """Tokenize multiple URLs."""
    return [tokenize_url(url) for url in urls]


def compute_cosine_similarity(
    vec1: np.ndarray,
    vec2: np.ndarray,
) -> float:
    """
    Compute cosine similarity between two vectors.

    Args:
        vec1: First vector
        vec2: Second vector

    Returns:
        Cosine similarity in range [-1, 1]
    """
    dot_product = np.dot(vec1, vec2)
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return float(dot_product / (norm1 * norm2))


def compute_pairwise_similarity(embeddings: np.ndarray) -> np.ndarray:
    """
    Compute pairwise cosine similarity matrix.

    Args:
        embeddings: Array of shape (n_samples, n_features)

    Returns:
        Similarity matrix of shape (n_samples, n_samples)
    """
    if SKLEARN_AVAILABLE:
        return sklearn_cosine_similarity(embeddings)
    else:
        # Manual computation
        n = embeddings.shape[0]
        sim_matrix = np.zeros((n, n))
        for i in range(n):
            for j in range(i, n):
                sim = compute_cosine_similarity(embeddings[i], embeddings[j])
                sim_matrix[i, j] = sim
                sim_matrix[j, i] = sim
        return sim_matrix


def compute_bundle_similarity(
    embeddings: np.ndarray,
    include_matrix: bool = False,
) -> SimilarityResult:
    """
    Compute similarity statistics for a bundle of URL embeddings.

    Args:
        embeddings: Array of shape (n_urls, n_features)
        include_matrix: Whether to include full similarity matrix

    Returns:
        SimilarityResult with statistics
    """
    n = embeddings.shape[0]

    if n < 2:
        return SimilarityResult(
            mean_similarity=1.0,
            min_similarity=1.0,
            max_similarity=1.0,
            std_similarity=0.0,
            pairwise_count=0,
            similarity_matrix=np.array([[1.0]]) if include_matrix else None,
        )

    sim_matrix = compute_pairwise_similarity(embeddings)

    # Extract upper triangle (excluding diagonal)
    upper_indices = np.triu_indices(n, k=1)
    pairwise_sims = sim_matrix[upper_indices]

    return SimilarityResult(
        mean_similarity=float(np.mean(pairwise_sims)),
        min_similarity=float(np.min(pairwise_sims)),
        max_similarity=float(np.max(pairwise_sims)),
        std_similarity=float(np.std(pairwise_sims)),
        pairwise_count=len(pairwise_sims),
        similarity_matrix=sim_matrix if include_matrix else None,
    )


class URLEmbedder:
    """
    Generate embeddings for URLs using various strategies.

    Supports:
    - TF-IDF: Fast, interpretable, no external dependencies
    - Sentence Transformers: Rich semantic embeddings (requires GPU for scale)
    """

    def __init__(
        self,
        method: str = "tfidf",
        model_name: str = "all-MiniLM-L6-v2",
        **kwargs,
    ):
        """
        Initialize URL embedder.

        Args:
            method: Embedding method ('tfidf' or 'transformer')
            model_name: Model name for transformer method
            **kwargs: Additional arguments for the embedding method
        """
        self.method = method
        self.model_name = model_name
        self._vectorizer: Optional[TfidfVectorizer] = None
        self._transformer: Optional["SentenceTransformer"] = None
        self._fitted = False

        if method == "transformer" and not SENTENCE_TRANSFORMERS_AVAILABLE:
            logger.warning(
                "sentence-transformers not available, falling back to TF-IDF"
            )
            self.method = "tfidf"

        if method == "tfidf" and not SKLEARN_AVAILABLE:
            raise ImportError(
                "scikit-learn is required for TF-IDF embedding. "
                "Install with: pip install scikit-learn"
            )

    def fit(self, urls: list[str]) -> "URLEmbedder":
        """
        Fit the embedder on a corpus of URLs.

        Required for TF-IDF, optional for transformers.
        URLs are deduplicated before fitting.

        Args:
            urls: List of URLs to fit on

        Returns:
            self for method chaining
        """
        # Deduplicate URLs before fitting
        unique_urls = list(set(urls))
        tokenized = tokenize_urls(unique_urls)

        if self.method == "tfidf":
            self._vectorizer = TfidfVectorizer(
                max_features=1000,
                ngram_range=(1, 2),
                min_df=1,
                max_df=0.95,
            )
            self._vectorizer.fit(tokenized)
            logger.info(
                f"Fitted TF-IDF vectorizer on {len(unique_urls)} unique URLs "
                f"(from {len(urls)} total)"
            )

        elif self.method == "transformer":
            # Transformers don't need fitting, but we load the model here
            if self._transformer is None:
                logger.info(f"Loading transformer model: {self.model_name}")
                self._transformer = SentenceTransformer(self.model_name)

        self._fitted = True
        return self

    def embed(self, urls: Union[str, list[str]]) -> np.ndarray:
        """
        Generate embeddings for URLs.

        Args:
            urls: Single URL or list of URLs

        Returns:
            Embeddings array of shape (n_urls, n_features)
        """
        if isinstance(urls, str):
            urls = [urls]

        tokenized = tokenize_urls(urls)

        if self.method == "tfidf":
            if self._vectorizer is None:
                # Auto-fit if not fitted
                self.fit(urls)
            embeddings = self._vectorizer.transform(tokenized).toarray()

        elif self.method == "transformer":
            if self._transformer is None:
                logger.info(f"Loading transformer model: {self.model_name}")
                self._transformer = SentenceTransformer(self.model_name)
            embeddings = self._transformer.encode(
                tokenized,
                show_progress_bar=False,
                convert_to_numpy=True,
            )

        else:
            raise ValueError(f"Unknown method: {self.method}")

        return embeddings

    def compute_similarity(
        self,
        urls: list[str],
        include_matrix: bool = False,
    ) -> SimilarityResult:
        """
        Compute similarity for a set of URLs.

        Args:
            urls: List of URLs
            include_matrix: Whether to include full similarity matrix

        Returns:
            SimilarityResult with statistics
        """
        if len(urls) < 2:
            return SimilarityResult(
                mean_similarity=1.0,
                min_similarity=1.0,
                max_similarity=1.0,
                std_similarity=0.0,
                pairwise_count=0,
            )

        embeddings = self.embed(urls)
        return compute_bundle_similarity(embeddings, include_matrix=include_matrix)


# Re-export get_confidence_level from schemas for backwards compatibility
# The canonical implementation with CONFIDENCE_THRESHOLDS is in schemas/bundles.py
from llm_bot_pipeline.schemas.bundles import (  # noqa: E402
    CONFIDENCE_THRESHOLDS,
    get_confidence_level,
)


class BundleSimilarityAnalyzer:
    """
    Analyze semantic similarity across multiple bundles.

    Provides batch processing and aggregate statistics.
    """

    def __init__(
        self,
        embedder: Optional[URLEmbedder] = None,
        method: str = "tfidf",
    ):
        """
        Initialize bundle similarity analyzer.

        Args:
            embedder: Pre-configured URLEmbedder (optional)
            method: Embedding method if creating new embedder
        """
        self.embedder = embedder or URLEmbedder(method=method)
        self._results: list[SimilarityResult] = []

    def fit_on_bundles(self, bundles: list) -> "BundleSimilarityAnalyzer":
        """
        Fit embedder on all URLs across bundles.

        Args:
            bundles: List of Bundle objects with 'urls' attribute

        Returns:
            self for method chaining
        """
        all_urls = []
        for bundle in bundles:
            all_urls.extend(bundle.urls)

        if all_urls:
            self.embedder.fit(all_urls)

        return self

    def analyze_bundles(
        self,
        bundles: list,
        include_matrices: bool = False,
    ) -> list[SimilarityResult]:
        """
        Compute similarity for multiple bundles.

        Args:
            bundles: List of Bundle objects with 'urls' attribute
            include_matrices: Whether to include similarity matrices

        Returns:
            List of SimilarityResult, one per bundle
        """
        results = []

        for bundle in bundles:
            result = self.embedder.compute_similarity(
                bundle.urls,
                include_matrix=include_matrices,
            )
            results.append(result)

        self._results = results
        return results

    def get_aggregate_stats(self) -> dict:
        """
        Get aggregate statistics across all analyzed bundles.

        Returns:
            Dictionary with aggregate metrics
        """
        if not self._results:
            return {}

        mean_sims = [r.mean_similarity for r in self._results]
        min_sims = [r.min_similarity for r in self._results]

        # Count by confidence level
        confidence_counts = {"high": 0, "medium": 0, "low": 0}
        for r in self._results:
            level = get_confidence_level(r.mean_similarity, r.min_similarity)
            confidence_counts[level] += 1

        return {
            "bundle_count": len(self._results),
            "mean_of_means": float(np.mean(mean_sims)),
            "std_of_means": float(np.std(mean_sims)),
            "mean_of_mins": float(np.mean(min_sims)),
            "overall_min": float(min(min_sims)),
            "overall_max": float(max([r.max_similarity for r in self._results])),
            "confidence_distribution": confidence_counts,
            "high_confidence_rate": confidence_counts["high"] / len(self._results),
        }
