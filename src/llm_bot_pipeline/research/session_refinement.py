"""
Session refinement for collision detection and semantic splitting.

Provides graph-based algorithms to detect and split bundles that may
contain requests from multiple sources (collisions) using URL similarity
and IP-based signals.

This module builds on:
- EnrichedBundle from temporal_analysis.py for bundle structure
- URLEmbedder from semantic_embeddings.py for similarity computation
- CollisionCandidate from fingerprint_analysis.py for detection

Key concepts:
- Similarity Graph: Nodes are requests, edges connect similar URLs
- Connected Components: Sub-bundles that should remain together
- MIBCS (Mean Intra-Bundle Cosine Similarity): Coherence metric
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from .semantic_embeddings import URLEmbedder, compute_pairwise_similarity
from .temporal_analysis import EnrichedBundle

logger = logging.getLogger(__name__)


def get_ip_network(ip: Optional[str]) -> Optional[str]:
    """
    Extract IP network (first 3 octets) from an IP address.

    Args:
        ip: IP address string (IPv4)

    Returns:
        Network string (e.g., "192.168.1") or None if invalid
    """
    if not ip:
        return None
    parts = ip.split(".")
    if len(parts) >= 3:
        return ".".join(parts[:3])
    return None


__all__ = [
    "RefinementResult",
    "SplitCandidate",
    "SessionRefiner",
    "UnionFind",
    "build_similarity_graph",
    "find_connected_components",
]


# =============================================================================
# Dataclasses
# =============================================================================


@dataclass
class SplitCandidate:
    """
    A candidate sub-bundle from splitting a collision.

    Attributes:
        request_indices: Indices of requests in this sub-bundle (relative to parent)
        urls: URLs in this sub-bundle
        client_ips: Client IPs in this sub-bundle
        mean_similarity: Mean pairwise URL similarity within sub-bundle
        ip_count: Number of unique IPs in sub-bundle
    """

    request_indices: list[int]
    urls: list[str]
    client_ips: list[str]
    mean_similarity: float
    ip_count: int

    @property
    def request_count(self) -> int:
        """Number of requests in this split."""
        return len(self.request_indices)

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "request_indices": self.request_indices,
            "url_count": len(self.urls),
            "mean_similarity": self.mean_similarity,
            "ip_count": self.ip_count,
            "request_count": self.request_count,
        }


@dataclass
class RefinementResult:
    """
    Result of attempting to refine (split) a bundle.

    Attributes:
        original_bundle: The bundle that was analyzed
        was_split: Whether the bundle was actually split
        split_reason: Reason for splitting (or not splitting)
        sub_bundles: List of resulting sub-bundles (if split)
        original_mibcs: Original mean intra-bundle cosine similarity
        refined_mibcs: Mean MIBCS across sub-bundles after split
        mibcs_improvement: Improvement in MIBCS (refined - original)
        original_ip_count: Original number of unique IPs
        refined_max_ip_count: Maximum unique IPs in any sub-bundle
        ip_improvement: Whether IP homogeneity improved
    """

    original_bundle: EnrichedBundle
    was_split: bool
    split_reason: str
    sub_bundles: list[EnrichedBundle] = field(default_factory=list)
    original_mibcs: float = 0.0
    refined_mibcs: float = 0.0
    mibcs_improvement: float = 0.0
    original_ip_count: int = 0
    refined_max_ip_count: int = 0
    ip_improvement: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "bundle_id": self.original_bundle.bundle_id,
            "was_split": self.was_split,
            "split_reason": self.split_reason,
            "sub_bundle_count": len(self.sub_bundles),
            "sub_bundle_ids": [b.bundle_id for b in self.sub_bundles],
            "original_mibcs": self.original_mibcs,
            "refined_mibcs": self.refined_mibcs,
            "mibcs_improvement": self.mibcs_improvement,
            "original_ip_count": self.original_ip_count,
            "refined_max_ip_count": self.refined_max_ip_count,
            "ip_improvement": self.ip_improvement,
        }


# =============================================================================
# Graph Construction
# =============================================================================


def build_similarity_graph(
    embeddings: np.ndarray,
    threshold: float = 0.5,
) -> list[list[int]]:
    """
    Build adjacency list representation of similarity graph.

    Creates edges between URLs with similarity >= threshold.

    Args:
        embeddings: URL embeddings array (n_urls, n_features)
        threshold: Minimum similarity to create an edge

    Returns:
        Adjacency list: adj[i] = list of nodes connected to node i
    """
    n = embeddings.shape[0]
    if n < 2:
        return [[]]

    # Compute pairwise similarities
    sim_matrix = compute_pairwise_similarity(embeddings)

    # Build adjacency list
    adj: list[list[int]] = [[] for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            if sim_matrix[i, j] >= threshold:
                adj[i].append(j)
                adj[j].append(i)

    return adj


# =============================================================================
# Connected Components (Union-Find)
# =============================================================================


class UnionFind:
    """
    Union-Find data structure for connected components.

    Provides efficient find and union operations with path compression
    and union by rank optimizations.
    """

    def __init__(self, n: int):
        """Initialize with n disjoint elements."""
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        """Find root of x with path compression."""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: int, y: int) -> None:
        """Union sets containing x and y."""
        px, py = self.find(x), self.find(y)
        if px == py:
            return

        # Union by rank
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        self.parent[py] = px
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1

    def get_components(self) -> list[list[int]]:
        """Get all connected components as lists of indices."""
        n = len(self.parent)
        components: dict[int, list[int]] = {}

        for i in range(n):
            root = self.find(i)
            if root not in components:
                components[root] = []
            components[root].append(i)

        return list(components.values())


def find_connected_components(adj: list[list[int]]) -> list[list[int]]:
    """
    Find connected components in a graph.

    Args:
        adj: Adjacency list representation

    Returns:
        List of components, each component is a list of node indices
    """
    n = len(adj)
    if n == 0:
        return []

    uf = UnionFind(n)

    for i, neighbors in enumerate(adj):
        for j in neighbors:
            uf.union(i, j)

    return uf.get_components()


# =============================================================================
# Session Refiner
# =============================================================================


class SessionRefiner:
    """
    Refine temporal bundles by splitting collisions using semantic similarity.

    Uses graph-based algorithms to identify clusters of semantically related
    URLs within a bundle, then splits bundles where distinct clusters exist.

    Example:
        >>> refiner = SessionRefiner()
        >>> result = refiner.refine_bundle(collision_bundle)
        >>> if result.was_split:
        ...     for sub_bundle in result.sub_bundles:
        ...         print(f"Sub-bundle: {sub_bundle.bundle_id}")
    """

    def __init__(
        self,
        similarity_threshold: float = 0.5,
        min_sub_bundle_size: int = 2,
        min_mibcs_improvement: float = 0.05,
        embedder: Optional[URLEmbedder] = None,
        use_ip_signal: bool = True,
    ):
        """
        Initialize session refiner.

        Args:
            similarity_threshold: Minimum URL similarity for graph edges
            min_sub_bundle_size: Minimum requests per sub-bundle after split
            min_mibcs_improvement: Minimum MIBCS improvement to accept split
            embedder: Pre-configured URLEmbedder (creates TF-IDF if None)
            use_ip_signal: Whether to use IP-based signals for splitting
                (reserved for future implementation)
        """
        self.similarity_threshold = similarity_threshold
        self.min_sub_bundle_size = min_sub_bundle_size
        self.min_mibcs_improvement = min_mibcs_improvement
        self.embedder = embedder or URLEmbedder(method="tfidf")
        self.use_ip_signal = use_ip_signal  # Reserved for future IP-based refinement

    def compute_mibcs(self, urls: list[str]) -> float:
        """
        Compute Mean Intra-Bundle Cosine Similarity for URLs.

        Args:
            urls: List of URLs

        Returns:
            Mean pairwise cosine similarity (0-1)
        """
        if len(urls) < 2:
            return 1.0

        result = self.embedder.compute_similarity(urls)
        return result.mean_similarity

    def build_graph(
        self,
        urls: list[str],
    ) -> tuple[np.ndarray, list[list[int]]]:
        """
        Build similarity graph from URLs.

        Args:
            urls: List of URLs

        Returns:
            Tuple of (embeddings array, adjacency list)
        """
        if len(urls) < 2:
            return np.array([]), [[]]

        embeddings = self.embedder.embed(urls)
        adj = build_similarity_graph(embeddings, self.similarity_threshold)

        return embeddings, adj

    def find_splits(
        self,
        bundle: EnrichedBundle,
    ) -> list[SplitCandidate]:
        """
        Find candidate splits for a bundle using URL similarity.

        Args:
            bundle: EnrichedBundle to analyze

        Returns:
            List of SplitCandidate objects (one per connected component)
        """
        urls = bundle.urls
        client_ips = bundle.client_ips

        if len(urls) < 2:
            return [
                SplitCandidate(
                    request_indices=[0] if urls else [],
                    urls=urls,
                    client_ips=client_ips,
                    mean_similarity=1.0,
                    ip_count=len(set(ip for ip in client_ips if ip)),
                )
            ]

        # Build similarity graph
        embeddings, adj = self.build_graph(urls)

        # Find connected components
        components = find_connected_components(adj)

        # Create split candidates
        splits = []
        for component in components:
            comp_urls = [urls[i] for i in component]
            comp_ips = [client_ips[i] if i < len(client_ips) else "" for i in component]

            # Compute similarity within component
            if len(comp_urls) >= 2:
                comp_embeddings = embeddings[component]
                sim_matrix = compute_pairwise_similarity(comp_embeddings)
                # Extract upper triangle
                upper_idx = np.triu_indices(len(component), k=1)
                mean_sim = (
                    float(np.mean(sim_matrix[upper_idx]))
                    if len(upper_idx[0]) > 0
                    else 1.0
                )
            else:
                mean_sim = 1.0

            splits.append(
                SplitCandidate(
                    request_indices=component,
                    urls=comp_urls,
                    client_ips=comp_ips,
                    mean_similarity=mean_sim,
                    ip_count=len(set(ip for ip in comp_ips if ip)),
                )
            )

        return splits

    def find_network_splits(
        self,
        bundle: EnrichedBundle,
    ) -> list[SplitCandidate]:
        """
        Find candidate splits for a bundle using IP network boundaries.

        Groups requests by IP network (first 3 octets), treating each network
        as a separate session. This is based on the hypothesis that LLMs
        dispatch queries to the same data center for a single user question.

        Args:
            bundle: EnrichedBundle to analyze

        Returns:
            List of SplitCandidate objects (one per unique network)
        """
        urls = bundle.urls
        client_ips = bundle.client_ips

        if len(urls) < 2:
            return [
                SplitCandidate(
                    request_indices=[0] if urls else [],
                    urls=urls,
                    client_ips=client_ips,
                    mean_similarity=1.0,
                    ip_count=len(set(ip for ip in client_ips if ip)),
                )
            ]

        # Group by IP network
        network_groups: dict[str, list[int]] = {}
        for i, ip in enumerate(client_ips):
            network = get_ip_network(ip) or "unknown"
            if network not in network_groups:
                network_groups[network] = []
            network_groups[network].append(i)

        # Create split candidates for each network
        splits = []
        for network, indices in network_groups.items():
            comp_urls = [urls[i] for i in indices]
            comp_ips = [client_ips[i] for i in indices]

            # Compute semantic similarity within network group
            if len(comp_urls) >= 2:
                embeddings = self.embedder.embed(comp_urls)
                sim_matrix = compute_pairwise_similarity(embeddings)
                upper_idx = np.triu_indices(len(indices), k=1)
                mean_sim = (
                    float(np.mean(sim_matrix[upper_idx]))
                    if len(upper_idx[0]) > 0
                    else 1.0
                )
            else:
                mean_sim = 1.0

            splits.append(
                SplitCandidate(
                    request_indices=indices,
                    urls=comp_urls,
                    client_ips=comp_ips,
                    mean_similarity=mean_sim,
                    ip_count=len(set(ip for ip in comp_ips if ip)),
                )
            )

        return splits

    def find_network_then_mibcs_splits(
        self,
        bundle: EnrichedBundle,
        host_threshold: int = 3,
    ) -> list[SplitCandidate]:
        """
        Two-stage splitting: first by network, then by MIBCS within network.

        Stage 1: Group by IP network (hard boundary)
        Stage 2: For network groups with ≥host_threshold unique hosts,
                 apply MIBCS-based splitting within that group

        Args:
            bundle: EnrichedBundle to analyze
            host_threshold: Minimum hosts in a network group to trigger MIBCS analysis

        Returns:
            List of SplitCandidate objects
        """
        urls = bundle.urls
        client_ips = bundle.client_ips

        if len(urls) < 2:
            return [
                SplitCandidate(
                    request_indices=[0] if urls else [],
                    urls=urls,
                    client_ips=client_ips,
                    mean_similarity=1.0,
                    ip_count=len(set(ip for ip in client_ips if ip)),
                )
            ]

        # Stage 1: Group by IP network
        network_groups: dict[str, list[int]] = {}
        for i, ip in enumerate(client_ips):
            network = get_ip_network(ip) or "unknown"
            if network not in network_groups:
                network_groups[network] = []
            network_groups[network].append(i)

        all_splits = []

        for network, indices in network_groups.items():
            comp_urls = [urls[i] for i in indices]
            comp_ips = [client_ips[i] for i in indices]

            # Count unique hosts (last octet) in this network group
            unique_hosts = set()
            for ip in comp_ips:
                if ip:
                    parts = ip.split(".")
                    if len(parts) == 4:
                        unique_hosts.add(parts[3])

            # Stage 2: If many hosts, apply MIBCS-based splitting within group
            if len(unique_hosts) >= host_threshold and len(comp_urls) >= 2:
                # Create a sub-bundle for MIBCS analysis
                sub_bundle = EnrichedBundle(
                    bundle_id=f"{bundle.bundle_id}_network_{network}",
                    start_time=bundle.start_time,
                    end_time=bundle.end_time,
                    request_count=len(indices),
                    bot_provider=bundle.bot_provider,
                    urls=comp_urls,
                    request_indices=list(range(len(indices))),
                    client_ips=comp_ips,
                    response_statuses=[],
                    bot_scores=[],
                    countries=[],
                    bot_tags=[],
                    bot_name=bundle.bot_name,
                )

                # Use MIBCS-based splitting on this network group
                sub_splits = self.find_splits(sub_bundle)

                # Map indices back to original bundle
                for sub_split in sub_splits:
                    original_indices = [indices[j] for j in sub_split.request_indices]
                    all_splits.append(
                        SplitCandidate(
                            request_indices=original_indices,
                            urls=sub_split.urls,
                            client_ips=sub_split.client_ips,
                            mean_similarity=sub_split.mean_similarity,
                            ip_count=sub_split.ip_count,
                        )
                    )
            else:
                # Keep network group as single split
                if len(comp_urls) >= 2:
                    embeddings = self.embedder.embed(comp_urls)
                    sim_matrix = compute_pairwise_similarity(embeddings)
                    upper_idx = np.triu_indices(len(indices), k=1)
                    mean_sim = (
                        float(np.mean(sim_matrix[upper_idx]))
                        if len(upper_idx[0]) > 0
                        else 1.0
                    )
                else:
                    mean_sim = 1.0

                all_splits.append(
                    SplitCandidate(
                        request_indices=indices,
                        urls=comp_urls,
                        client_ips=comp_ips,
                        mean_similarity=mean_sim,
                        ip_count=len(set(ip for ip in comp_ips if ip)),
                    )
                )

        return all_splits

    def validate_split(
        self,
        bundle: EnrichedBundle,
        splits: list[SplitCandidate],
    ) -> tuple[bool, str]:
        """
        Validate whether a split should be accepted.

        Checks:
        - Multiple components exist
        - All sub-bundles meet minimum size
        - MIBCS improvement exceeds threshold

        Args:
            bundle: Original bundle
            splits: Proposed split candidates

        Returns:
            Tuple of (is_valid, reason)
        """
        # Must have multiple components
        if len(splits) <= 1:
            return False, "Single component - no split possible"

        # Check minimum size
        for i, split in enumerate(splits):
            if split.request_count < self.min_sub_bundle_size:
                return (
                    False,
                    f"Sub-bundle {i} has {split.request_count} requests (min: {self.min_sub_bundle_size})",
                )

        # Compute MIBCS improvement
        original_mibcs = self.compute_mibcs(bundle.urls)
        refined_mibcs_values = [
            s.mean_similarity for s in splits if s.request_count >= 2
        ]

        if not refined_mibcs_values:
            return False, "No multi-request sub-bundles for MIBCS calculation"

        refined_mibcs = float(np.mean(refined_mibcs_values))
        improvement = refined_mibcs - original_mibcs

        if improvement < self.min_mibcs_improvement:
            return (
                False,
                f"MIBCS improvement {improvement:.3f} below threshold {self.min_mibcs_improvement}",
            )

        return True, f"Valid split: {len(splits)} components, MIBCS +{improvement:.3f}"

    def create_sub_bundles(
        self,
        bundle: EnrichedBundle,
        splits: list[SplitCandidate],
    ) -> list[EnrichedBundle]:
        """
        Create EnrichedBundle objects for each split.

        Args:
            bundle: Original bundle
            splits: Validated split candidates

        Returns:
            List of EnrichedBundle objects
        """
        sub_bundles = []

        for i, split in enumerate(splits):
            # Get timestamps for this split
            # Note: We don't have per-request timestamps in EnrichedBundle,
            # so we use the original bundle's time range
            sub_bundle = EnrichedBundle(
                bundle_id=f"{bundle.bundle_id}_split_{i}",
                start_time=bundle.start_time,
                end_time=bundle.end_time,
                request_count=split.request_count,
                bot_provider=bundle.bot_provider,
                urls=split.urls,
                request_indices=split.request_indices,
                client_ips=split.client_ips,
                response_statuses=[
                    bundle.response_statuses[j]
                    for j in split.request_indices
                    if j < len(bundle.response_statuses)
                ],
                bot_scores=[
                    bundle.bot_scores[j]
                    for j in split.request_indices
                    if j < len(bundle.bot_scores)
                ],
                countries=[
                    bundle.countries[j]
                    for j in split.request_indices
                    if j < len(bundle.countries)
                ],
                bot_tags=[
                    bundle.bot_tags[j]
                    for j in split.request_indices
                    if j < len(bundle.bot_tags)
                ],
                bot_name=bundle.bot_name,
            )
            sub_bundles.append(sub_bundle)

        return sub_bundles

    def refine_bundle(
        self,
        bundle: EnrichedBundle,
        strategy: str = "mibcs_only",
        network_host_threshold: int = 3,
    ) -> RefinementResult:
        """
        Attempt to refine (split) a bundle based on the specified strategy.

        Strategies:
        - mibcs_only: Use semantic similarity (default, original behavior)
        - network_only: Split by IP network (first 3 octets)
        - network_then_mibcs: Network first, then MIBCS within high-host groups
        - mibcs_then_network: MIBCS first, validate network consistency

        Args:
            bundle: EnrichedBundle to refine
            strategy: Splitting strategy to use
            network_host_threshold: Min hosts to trigger MIBCS in network_then_mibcs

        Returns:
            RefinementResult with split details
        """
        # Compute original metrics
        original_mibcs = self.compute_mibcs(bundle.urls)
        original_ip_count = len(bundle.unique_ips)

        # Find candidate splits based on strategy
        if strategy == "network_only":
            splits = self.find_network_splits(bundle)
            split_method = "network"
        elif strategy == "network_then_mibcs":
            splits = self.find_network_then_mibcs_splits(bundle, network_host_threshold)
            split_method = "network_then_mibcs"
        elif strategy == "mibcs_then_network":
            # Apply MIBCS first
            mibcs_splits = self.find_splits(bundle)
            # Then validate each split is network-consistent
            # For now, just use MIBCS (network validation can be added later)
            splits = mibcs_splits
            split_method = "mibcs_then_network"
        else:  # mibcs_only (default)
            splits = self.find_splits(bundle)
            split_method = "mibcs"

        # Validate split (skip MIBCS improvement check for network_only)
        if strategy == "network_only":
            # For network-only, just check we have multiple networks
            if len(splits) <= 1:
                is_valid, reason = False, "Single network - no split possible"
            else:
                # Check minimum size
                is_valid = True
                reason = f"Network split: {len(splits)} networks"
                for i, split in enumerate(splits):
                    if split.request_count < self.min_sub_bundle_size:
                        is_valid = False
                        reason = f"Sub-bundle {i} has {split.request_count} requests (min: {self.min_sub_bundle_size})"
                        break
        else:
            is_valid, reason = self.validate_split(bundle, splits)

        # Prepend strategy to reason
        reason = f"[{split_method}] {reason}"

        if not is_valid:
            return RefinementResult(
                original_bundle=bundle,
                was_split=False,
                split_reason=reason,
                original_mibcs=original_mibcs,
                refined_mibcs=original_mibcs,
                mibcs_improvement=0.0,
                original_ip_count=original_ip_count,
                refined_max_ip_count=original_ip_count,
                ip_improvement=False,
            )

        # Create sub-bundles
        sub_bundles = self.create_sub_bundles(bundle, splits)

        # Compute refined metrics
        refined_mibcs_values = [
            self.compute_mibcs(sb.urls) for sb in sub_bundles if sb.request_count >= 2
        ]
        refined_mibcs = (
            float(np.mean(refined_mibcs_values)) if refined_mibcs_values else 1.0
        )
        mibcs_improvement = refined_mibcs - original_mibcs

        refined_max_ip_count = max(len(sb.unique_ips) for sb in sub_bundles)
        ip_improvement = refined_max_ip_count < original_ip_count

        return RefinementResult(
            original_bundle=bundle,
            was_split=True,
            split_reason=reason,
            sub_bundles=sub_bundles,
            original_mibcs=original_mibcs,
            refined_mibcs=refined_mibcs,
            mibcs_improvement=mibcs_improvement,
            original_ip_count=original_ip_count,
            refined_max_ip_count=refined_max_ip_count,
            ip_improvement=ip_improvement,
        )

    def refine_bundles(
        self,
        bundles: list[EnrichedBundle],
        only_collisions: bool = True,
        collision_ip_threshold: int = 2,
        strategy: str = "mibcs_only",
        network_host_threshold: int = 3,
    ) -> list[RefinementResult]:
        """
        Refine multiple bundles using the specified strategy.

        Args:
            bundles: List of bundles to refine
            only_collisions: If True, only refine bundles with multiple IPs
                (ignored for network_only strategy)
            collision_ip_threshold: Minimum unique IPs to consider for refinement
            strategy: Splitting strategy ("mibcs_only", "network_only",
                "network_then_mibcs", "mibcs_then_network")
            network_host_threshold: Min hosts to trigger MIBCS in network_then_mibcs

        Returns:
            List of RefinementResult objects
        """
        # Fit embedder on all URLs for consistent representation
        all_urls = []
        for bundle in bundles:
            all_urls.extend(bundle.urls)
        if all_urls:
            self.embedder.fit(all_urls)

        results = []
        for bundle in bundles:
            # For network_only strategy, process all bundles (network is the signal)
            # For other strategies, optionally skip non-collision bundles
            if strategy != "network_only" and only_collisions:
                unique_ip_count = len(bundle.unique_ips)
                if unique_ip_count < collision_ip_threshold:
                    # Not a collision candidate - skip refinement
                    results.append(
                        RefinementResult(
                            original_bundle=bundle,
                            was_split=False,
                            split_reason=f"Not a collision ({unique_ip_count} unique IPs < threshold {collision_ip_threshold})",
                            original_mibcs=self.compute_mibcs(bundle.urls),
                            original_ip_count=unique_ip_count,
                            refined_max_ip_count=unique_ip_count,
                        )
                    )
                    continue

            result = self.refine_bundle(
                bundle,
                strategy=strategy,
                network_host_threshold=network_host_threshold,
            )
            results.append(result)

        return results

    def get_refinement_summary(
        self,
        results: list[RefinementResult],
    ) -> dict:
        """
        Get summary statistics for refinement results.

        Args:
            results: List of RefinementResult objects

        Returns:
            Dictionary with summary statistics
        """
        total = len(results)
        if total == 0:
            return {
                "total_bundles": 0,
                "bundles_split": 0,
                "split_rate": 0.0,
                "total_sub_bundles": 0,
                "mean_mibcs_improvement": 0.0,
                "bundles_with_ip_improvement": 0,
            }

        split_results = [r for r in results if r.was_split]
        bundles_split = len(split_results)

        total_sub_bundles = sum(len(r.sub_bundles) for r in split_results)
        mibcs_improvements = [r.mibcs_improvement for r in split_results]
        ip_improvements = sum(1 for r in split_results if r.ip_improvement)

        return {
            "total_bundles": total,
            "bundles_split": bundles_split,
            "split_rate": bundles_split / total,
            "total_sub_bundles": total_sub_bundles,
            "mean_mibcs_improvement": (
                float(np.mean(mibcs_improvements)) if mibcs_improvements else 0.0
            ),
            "max_mibcs_improvement": (
                float(max(mibcs_improvements)) if mibcs_improvements else 0.0
            ),
            "bundles_with_ip_improvement": ip_improvements,
            "ip_improvement_rate": (
                ip_improvements / bundles_split if bundles_split > 0 else 0.0
            ),
        }
