"""
Query fan-out session aggregations for reporting.

SessionAggregator is a thin orchestrator that composes:
- TemporalBundler  (Stage 1: time-window bundling)
- SemanticRefiner  (Stage 2: optional collision-based splitting)
- SessionStorageWriter (Stage 3: persistence to SQLite / BigQuery)

Flow: raw data -> TemporalBundler.bundle_by_time()
      -> optional semantic refinement -> SessionStorageWriter.insert_sessions()
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from ..config.settings import SessionRefinementSettings
from ..research.fingerprint_analysis import FingerprintAnalyzer
from ..research.semantic_embeddings import URLEmbedder, get_confidence_level
from ..research.session_refinement import SessionRefiner as SemanticRefiner
from ..research.temporal_analysis import Bundle, EnrichedBundle
from ..storage import StorageBackend, get_backend
from ..utils.url_utils import derive_session_name
from .session_storage_writer import SessionStorageWriter
from .temporal_bundler import TemporalBundler

logger = logging.getLogger(__name__)


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
    fanout_session_name: Optional[str]  # Human-readable topic from first URL
    url_list: str  # JSON array
    window_ms: float
    domain: Optional[str] = None
    splitting_strategy: Optional[str] = None  # 'mibcs_only', 'network_only', etc.
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
    # Refinement metrics (optional)
    refinement: RefinementMetrics = field(default_factory=RefinementMetrics)


class SessionAggregator:
    """
    Thin orchestrator for query fan-out session creation.

    Composes three reporting-layer components:
    - **TemporalBundler**: groups requests into time-based bundles (Stage 1)
    - **SemanticRefiner**: optional collision detection & splitting (Stage 2)
    - **SessionStorageWriter**: persists session records (Stage 3)
    """

    # Table name for query fan-out sessions
    TABLE_NAME = "query_fanout_sessions"

    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        backend_type: str = "sqlite",
        db_path: Optional[Path] = None,
        embedding_method: str = "tfidf",
        refinement_settings: Optional[SessionRefinementSettings] = None,
    ):
        """
        Initialize session aggregator.

        Args:
            backend: Pre-initialized StorageBackend (optional)
            backend_type: Backend type if creating new ('sqlite' or 'bigquery')
            db_path: Path to SQLite database (for sqlite backend)
            embedding_method: Method for URL embeddings ('tfidf' or 'transformer')
            refinement_settings: Configuration for session refinement (optional)
        """
        if backend:
            self._backend = backend
            self._owns_backend = False
        else:
            kwargs = {}
            if backend_type == "sqlite" and db_path:
                kwargs["db_path"] = db_path
            self._backend = get_backend(backend_type, **kwargs)
            self._owns_backend = True

        self._embedder = URLEmbedder(method=embedding_method)
        self._refinement_settings = refinement_settings or SessionRefinementSettings()
        self._initialized = False

        # Composed components (orchestrator delegates to these)
        self._storage_writer = SessionStorageWriter(self._backend)

        # Initialize semantic refiner lazily (only when refinement is enabled)
        self._refiner: Optional[SemanticRefiner] = None
        self._fingerprint_analyzer: Optional[FingerprintAnalyzer] = None

        logger.info(
            f"SessionAggregator initialized with {self._backend.backend_type} backend, "
            f"refinement={'enabled' if self._refinement_settings.enabled else 'disabled'}"
        )

    def initialize(self) -> None:
        """Initialize the backend (create tables if needed)."""
        if not self._initialized:
            self._backend.initialize()
            self._initialized = True

    def close(self) -> None:
        """Close the backend connection."""
        if self._owns_backend:
            self._backend.close()

    def _get_table_ref(self) -> str:
        """Get the table reference for SQL queries.

        For SQLite, returns the simple table name.
        For BigQuery, returns the fully qualified table ID.
        """
        full_id = self._backend.get_full_table_id(self.TABLE_NAME)
        if self._backend.backend_type == "bigquery":
            return f"`{full_id}`"
        return full_id

    def __enter__(self) -> "SessionAggregator":
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def _get_refiner(self) -> SemanticRefiner:
        """Get or create the SemanticRefiner instance for collision splitting."""
        if self._refiner is None:
            self._refiner = SemanticRefiner(
                similarity_threshold=self._refinement_settings.similarity_threshold,
                min_sub_bundle_size=self._refinement_settings.min_sub_bundle_size,
                min_mibcs_improvement=self._refinement_settings.min_mibcs_improvement,
                embedder=self._embedder,
            )
        return self._refiner

    def _get_fingerprint_analyzer(self) -> FingerprintAnalyzer:
        """Get or create the FingerprintAnalyzer instance."""
        if self._fingerprint_analyzer is None:
            self._fingerprint_analyzer = FingerprintAnalyzer(
                collision_ip_threshold=self._refinement_settings.collision_ip_threshold,
                collision_homogeneity_threshold=self._refinement_settings.collision_homogeneity_threshold,
            )
        return self._fingerprint_analyzer

    def _create_enriched_bundles(
        self,
        bundles: list[Bundle],
        df: pd.DataFrame,
    ) -> list[EnrichedBundle]:
        """
        Convert Bundle objects to EnrichedBundle objects with fingerprint data.

        Args:
            bundles: List of basic Bundle objects
            df: Original DataFrame with request data

        Returns:
            List of EnrichedBundle objects
        """
        enriched_bundles = []

        for bundle in bundles:
            if not bundle.request_indices:
                continue

            bundle_df = df.iloc[bundle.request_indices]

            # Extract fingerprint fields (handle missing columns gracefully)
            client_ips = (
                bundle_df["client_ip"].tolist()
                if "client_ip" in bundle_df.columns
                else []
            )
            response_statuses = (
                bundle_df["response_status"].tolist()
                if "response_status" in bundle_df.columns
                else []
            )
            bot_scores = (
                bundle_df["bot_score"].tolist()
                if "bot_score" in bundle_df.columns
                else []
            )
            countries = (
                bundle_df["client_country"].tolist()
                if "client_country" in bundle_df.columns
                else []
            )
            bot_tags = (
                bundle_df["bot_tags"].tolist()
                if "bot_tags" in bundle_df.columns
                else []
            )
            bot_name = (
                bundle_df["bot_name"].iloc[0]
                if "bot_name" in bundle_df.columns and len(bundle_df) > 0
                else None
            )

            enriched = EnrichedBundle.from_bundle(
                bundle,
                client_ips=client_ips,
                response_statuses=response_statuses,
                bot_scores=bot_scores,
                countries=countries,
                bot_tags=bot_tags,
                bot_name=bot_name,
            )
            enriched_bundles.append(enriched)

        return enriched_bundles

    def _apply_refinement(
        self,
        bundles: list[Bundle],
        df: pd.DataFrame,
    ) -> tuple[list[Bundle], RefinementMetrics, dict[str, float]]:
        """
        Apply two-stage refinement: detect collisions and split them.

        Args:
            bundles: Original temporal bundles
            df: Original DataFrame

        Returns:
            Tuple of (refined bundles, refinement metrics, parent_mibcs_map)
            parent_mibcs_map maps parent bundle_id to pre-refinement MIBCS
        """
        metrics = RefinementMetrics(enabled=True)
        parent_mibcs_map: dict[str, float] = {}

        # Convert to EnrichedBundle for fingerprint analysis
        enriched_bundles = self._create_enriched_bundles(bundles, df)
        metrics.bundles_analyzed = len(enriched_bundles)

        if not enriched_bundles:
            return bundles, metrics, parent_mibcs_map

        # Compute coherence scores (MIBCS) for all bundles with ≥2 unique URLs
        # Required for collision detection
        coherence_scores: dict[str, float] = {}
        for enriched in enriched_bundles:
            if len(set(enriched.urls)) >= 2:
                try:
                    sim_result = self._embedder.compute_similarity(enriched.urls)
                    coherence_scores[enriched.bundle_id] = sim_result.mean_similarity
                except Exception:
                    logger.debug(
                        "Similarity computation failed for bundle %s",
                        enriched.bundle_id,
                        exc_info=True,
                    )

        # Detect collision candidates
        analyzer = self._get_fingerprint_analyzer()
        collisions = analyzer.detect_collisions(enriched_bundles, coherence_scores)
        metrics.collision_candidates = len(collisions)

        if self._refinement_settings.log_refinement_details:
            logger.info(
                f"Refinement: {len(collisions)} collision candidates "
                f"out of {len(enriched_bundles)} bundles"
            )

        if not collisions or not self._refinement_settings.enable_semantic_refinement:
            # No collisions or semantic refinement disabled
            return bundles, metrics, parent_mibcs_map

        # Apply semantic refinement to collision candidates
        refiner = self._get_refiner()

        # Note: Embedder is already fit in create_sessions_from_dataframe

        # Track which bundles get refined and their original MIBCS
        collision_bundle_ids = {c.bundle.bundle_id for c in collisions}
        refined_bundles: list[Bundle] = []
        total_sub_bundles = 0
        mibcs_improvements = []

        for enriched in enriched_bundles:
            if enriched.bundle_id not in collision_bundle_ids:
                # Not a collision - keep as is
                refined_bundles.append(enriched)
                continue

            # Reuse pre-computed MIBCS from coherence_scores (avoid duplicate compute)
            pre_mibcs = coherence_scores.get(enriched.bundle_id)
            if pre_mibcs is not None:
                parent_mibcs_map[enriched.bundle_id] = pre_mibcs

            # Attempt refinement
            result = refiner.refine_bundle(enriched)

            if result.was_split:
                # Split successful - add sub-bundles
                # Note: pre_refinement_mibcs is retrieved via parent_mibcs_map in _bundles_to_records
                refined_bundles.extend(result.sub_bundles)
                total_sub_bundles += len(result.sub_bundles)
                mibcs_improvements.append(result.mibcs_improvement)
                metrics.bundles_split += 1

                if self._refinement_settings.log_refinement_details:
                    pre_mibcs_str = (
                        f"{pre_mibcs:.3f}" if pre_mibcs is not None else "N/A"
                    )
                    logger.debug(
                        f"Split bundle {enriched.bundle_id} into "
                        f"{len(result.sub_bundles)} sub-bundles "
                        f"(MIBCS improvement: {result.mibcs_improvement:.3f}, "
                        f"pre-refinement MIBCS: {pre_mibcs_str})"
                    )
            else:
                # Split not viable - keep original
                refined_bundles.append(enriched)

        metrics.sub_bundles_created = total_sub_bundles
        if mibcs_improvements:
            metrics.mean_mibcs_improvement = sum(mibcs_improvements) / len(
                mibcs_improvements
            )

        if self._refinement_settings.log_refinement_details:
            logger.info(
                f"Refinement complete: {metrics.bundles_split} bundles split, "
                f"{metrics.sub_bundles_created} sub-bundles created, "
                f"mean MIBCS improvement: {metrics.mean_mibcs_improvement:.3f}"
            )

        return refined_bundles, metrics, parent_mibcs_map

    def create_sessions_from_dataframe(
        self,
        df: pd.DataFrame,
        window_ms: float,
        timestamp_col: str = "datetime",
        url_col: str = "url",
        group_by: str = "bot_provider",
        bot_name_col: Optional[str] = "bot_name",
        enable_refinement: Optional[bool] = None,
    ) -> SessionAggregationResult:
        """
        Create sessions from a DataFrame of requests.

        Two-stage approach when refinement is enabled:
        - Stage 1: Temporal bundling using time windows
        - Stage 2: Semantic refinement to split collision bundles

        Args:
            df: DataFrame with request data
            window_ms: Time window for bundling in milliseconds
            timestamp_col: Name of timestamp column
            url_col: Name of URL column
            group_by: Column to group by (e.g., 'bot_provider')
            bot_name_col: Column with bot name (optional)
            enable_refinement: Override for refinement setting (None uses config)

        Returns:
            SessionAggregationResult with statistics
        """
        self.initialize()
        started_at = datetime.now(timezone.utc)

        # Determine if refinement is enabled
        refinement_enabled = (
            enable_refinement
            if enable_refinement is not None
            else self._refinement_settings.enabled
        )

        logger.info(
            f"Creating sessions with {window_ms}ms window from {len(df)} requests "
            f"(refinement={'enabled' if refinement_enabled else 'disabled'})"
        )

        try:
            # Stage 1: Temporal bundling (delegated to TemporalBundler)
            bundler = TemporalBundler(
                window_ms=window_ms,
                timestamp_col=timestamp_col,
                url_col=url_col,
                group_by=group_by,
            )
            bundle_result = bundler.bundle_by_time(df)
            bundles = bundle_result.bundles

            logger.info(f"Stage 1: Created {len(bundles)} temporal bundles")

            refinement_metrics = RefinementMetrics(enabled=refinement_enabled)

            if not bundles:
                return SessionAggregationResult(
                    success=True,
                    sessions_created=0,
                    total_requests_bundled=0,
                    window_ms=window_ms,
                    mean_session_size=0,
                    high_confidence_count=0,
                    medium_confidence_count=0,
                    low_confidence_count=0,
                    duration_seconds=(
                        datetime.now(timezone.utc) - started_at
                    ).total_seconds(),
                    refinement=refinement_metrics,
                )

            # Fit embedder on all URLs (needed for both refinement and similarity)
            all_urls = []
            for bundle in bundles:
                all_urls.extend(bundle.urls)
            if all_urls:
                self._embedder.fit(all_urls)

            # Stage 2: Apply refinement if enabled
            parent_mibcs_map: dict[str, float] = {}
            if refinement_enabled:
                refinement_start = datetime.now(timezone.utc)
                bundles, refinement_metrics, parent_mibcs_map = self._apply_refinement(
                    bundles, df
                )
                refinement_duration_ms = (
                    datetime.now(timezone.utc) - refinement_start
                ).total_seconds() * 1000
                logger.info(
                    f"Stage 2: {len(bundles)} bundles after refinement "
                    f"({refinement_metrics.bundles_split} split)"
                )
                # Log refinement run to audit table
                self._log_refinement_run(
                    refinement_metrics, window_ms, refinement_duration_ms
                )

            # Convert bundles to session records
            records = self._bundles_to_records(
                bundles,
                window_ms,
                df,
                bot_name_col,
                parent_mibcs_map,
                splitting_strategy=self._refinement_settings.splitting_strategy,
            )

            # Stage 3: Storage (delegated to SessionStorageWriter)
            sessions_created = self._storage_writer.insert_sessions(records)

            # Calculate statistics
            confidence_counts = {"high": 0, "medium": 0, "low": 0}
            for record in records:
                confidence_counts[record.confidence_level] += 1

            total_requests = sum(r.request_count for r in records)
            mean_size = total_requests / len(records) if records else 0

            duration = (datetime.now(timezone.utc) - started_at).total_seconds()

            partial_failures = len(records) - sessions_created
            if partial_failures > 0:
                logger.warning(
                    f"{partial_failures} of {len(records)} session inserts failed"
                )

            logger.info(
                f"Created {sessions_created} sessions in {duration:.1f}s "
                f"(high: {confidence_counts['high']}, "
                f"medium: {confidence_counts['medium']}, "
                f"low: {confidence_counts['low']})"
            )

            return SessionAggregationResult(
                success=(partial_failures == 0),
                sessions_created=sessions_created,
                total_requests_bundled=total_requests,
                window_ms=window_ms,
                mean_session_size=mean_size,
                high_confidence_count=confidence_counts["high"],
                medium_confidence_count=confidence_counts["medium"],
                low_confidence_count=confidence_counts["low"],
                duration_seconds=duration,
                refinement=refinement_metrics,
            )

        except Exception as e:
            logger.exception(f"Failed to create sessions: {e}")
            duration = (datetime.now(timezone.utc) - started_at).total_seconds()
            return SessionAggregationResult(
                success=False,
                sessions_created=0,
                total_requests_bundled=0,
                window_ms=window_ms,
                mean_session_size=0,
                high_confidence_count=0,
                medium_confidence_count=0,
                low_confidence_count=0,
                error=str(e),
                duration_seconds=duration,
            )

    def _bundles_to_records(
        self,
        bundles: list[Bundle],
        window_ms: float,
        df: pd.DataFrame,
        bot_name_col: Optional[str],
        parent_mibcs_map: Optional[dict[str, float]] = None,
        splitting_strategy: Optional[str] = None,
    ) -> list[SessionRecord]:
        """Convert Bundle objects to SessionRecord objects."""
        records = []
        now = datetime.now(timezone.utc).isoformat()
        parent_mibcs_map = parent_mibcs_map or {}

        for bundle in bundles:
            # Compute similarity
            if len(bundle.urls) >= 2:
                sim_result = self._embedder.compute_similarity(bundle.urls)
                mean_sim = sim_result.mean_similarity
                min_sim = sim_result.min_similarity
                max_sim = sim_result.max_similarity
                confidence = get_confidence_level(mean_sim, min_sim)
            else:
                mean_sim = 1.0
                min_sim = 1.0
                max_sim = 1.0
                confidence = "high"  # Single URL is perfectly coherent

            # Get bot name - prefer from EnrichedBundle, fall back to DataFrame
            # Note: For sub-bundles from refinement, request_indices are relative
            # to the parent bundle, so we must use EnrichedBundle.bot_name
            bot_name = None
            if isinstance(bundle, EnrichedBundle) and bundle.bot_name:
                bot_name = bundle.bot_name
            elif (
                bot_name_col
                and bundle.request_indices
                and "_split_" not in bundle.bundle_id
            ):
                # Only look up from DataFrame for original bundles (not sub-bundles)
                try:
                    first_idx = bundle.request_indices[0]
                    if first_idx < len(df) and bot_name_col in df.columns:
                        bot_name = df.iloc[first_idx][bot_name_col]
                except (IndexError, KeyError):
                    pass

            # Derive domain from DataFrame (first request in bundle)
            domain = None
            if "domain" in df.columns and bundle.request_indices:
                if "_split_" not in bundle.bundle_id:
                    try:
                        first_idx = bundle.request_indices[0]
                        if first_idx < len(df):
                            domain = df.iloc[first_idx]["domain"]
                    except (IndexError, KeyError):
                        pass
                elif isinstance(bundle, EnrichedBundle):
                    domain = getattr(bundle, "domain", None)

            # Derive session name from first URL
            session_name = None
            if bundle.urls:
                session_name = derive_session_name(bundle.urls[0])

            # Deduplicate URLs while preserving order (first occurrence)
            seen = set()
            unique_url_list = []
            for url in bundle.urls:
                if url not in seen:
                    seen.add(url)
                    unique_url_list.append(url)

            # Check if this is a sub-bundle from refinement
            parent_session_id = None
            was_refined = False
            refinement_reason = None
            pre_refinement_mibcs = None

            if "_split_" in bundle.bundle_id:
                # This is a sub-bundle - extract parent ID
                parent_session_id = bundle.bundle_id.rsplit("_split_", 1)[0]
                was_refined = True
                refinement_reason = "semantic_split"
                # Get parent's pre-refinement MIBCS
                pre_refinement_mibcs = parent_mibcs_map.get(parent_session_id)

            record = SessionRecord(
                session_id=bundle.bundle_id,
                session_date=bundle.start_time.date().isoformat(),
                domain=domain,
                session_start_time=bundle.start_time.isoformat(),
                session_end_time=bundle.end_time.isoformat(),
                duration_ms=bundle.duration_ms,
                bot_provider=bundle.bot_provider,
                bot_name=bot_name,
                request_count=bundle.request_count,
                unique_urls=len(unique_url_list),
                mean_cosine_similarity=mean_sim,
                min_cosine_similarity=min_sim,
                max_cosine_similarity=max_sim,
                confidence_level=confidence,
                fanout_session_name=session_name,
                url_list=json.dumps(unique_url_list),
                window_ms=window_ms,
                splitting_strategy=splitting_strategy,
                _created_at=now,
                parent_session_id=parent_session_id,
                was_refined=was_refined,
                refinement_reason=refinement_reason,
                pre_refinement_mibcs=pre_refinement_mibcs,
            )
            records.append(record)

        return records

    def delete_sessions(
        self,
        session_date: Optional[str] = None,
        bot_provider: Optional[str] = None,
    ) -> int:
        """
        Delete existing sessions (for reprocessing).

        Args:
            session_date: Delete sessions for this date only (YYYY-MM-DD)
            bot_provider: Delete sessions for this provider only

        Returns:
            Number of sessions deleted
        """
        self.initialize()
        return self._storage_writer.delete_sessions(
            session_date=session_date, bot_provider=bot_provider
        )

    def get_session_summary(self) -> dict:
        """
        Get summary statistics for all sessions.

        Returns:
            Dictionary with session statistics
        """
        self.initialize()

        table_ref = self._get_table_ref()
        sql = f"""
            SELECT
                COUNT(*) as total_sessions,
                SUM(request_count) as total_requests,
                AVG(request_count) as avg_session_size,
                AVG(mean_cosine_similarity) as avg_similarity,
                SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) as high_count,
                SUM(CASE WHEN confidence_level = 'medium' THEN 1 ELSE 0 END) as medium_count,
                SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) as low_count,
                MIN(session_date) as earliest_date,
                MAX(session_date) as latest_date
            FROM {table_ref}
        """

        try:
            results = self._backend.query(sql)
            if results:
                row = results[0]
                total = row.get("total_sessions", 0) or 0
                return {
                    "total_sessions": total,
                    "total_requests": row.get("total_requests", 0) or 0,
                    "avg_session_size": row.get("avg_session_size", 0) or 0,
                    "avg_similarity": row.get("avg_similarity", 0) or 0,
                    "high_confidence_rate": (
                        (row.get("high_count", 0) or 0) / total if total > 0 else 0
                    ),
                    "confidence_distribution": {
                        "high": row.get("high_count", 0) or 0,
                        "medium": row.get("medium_count", 0) or 0,
                        "low": row.get("low_count", 0) or 0,
                    },
                    "date_range": {
                        "earliest": row.get("earliest_date"),
                        "latest": row.get("latest_date"),
                    },
                }
            return {}
        except Exception as e:
            logger.warning(f"Failed to get session summary: {e}")
            return {"error": str(e)}

    def _get_refinement_log_table_ref(self) -> str:
        """Get fully qualified table reference for refinement log."""
        table_name = "session_refinement_log"
        full_id = self._backend.get_full_table_id(table_name)
        if self._backend.backend_type == "bigquery":
            return f"`{full_id}`"
        return full_id

    def _log_refinement_run(
        self,
        metrics: RefinementMetrics,
        window_ms: float,
        duration_ms: float,
    ) -> None:
        """
        Log refinement run details to session_refinement_log table.

        Args:
            metrics: Refinement metrics from the run
            window_ms: Window size used
            duration_ms: Duration of refinement in milliseconds
        """
        if not metrics.enabled:
            return

        table_ref = self._get_refinement_log_table_ref()
        sql = f"""
            INSERT INTO {table_ref} (
                window_ms, total_bundles, collision_candidates,
                bundles_split, sub_bundles_created, mean_mibcs_improvement,
                refinement_duration_ms, collision_ip_threshold,
                collision_homogeneity_threshold, similarity_threshold,
                min_sub_bundle_size, min_mibcs_improvement
            ) VALUES (
                :window_ms, :total_bundles, :collision_candidates,
                :bundles_split, :sub_bundles_created, :mean_mibcs_improvement,
                :refinement_duration_ms, :collision_ip_threshold,
                :collision_homogeneity_threshold, :similarity_threshold,
                :min_sub_bundle_size, :min_mibcs_improvement
            )
        """

        try:
            self._backend.execute(
                sql,
                {
                    "window_ms": window_ms,
                    "total_bundles": metrics.bundles_analyzed,
                    "collision_candidates": metrics.collision_candidates,
                    "bundles_split": metrics.bundles_split,
                    "sub_bundles_created": metrics.sub_bundles_created,
                    "mean_mibcs_improvement": metrics.mean_mibcs_improvement,
                    "refinement_duration_ms": duration_ms,
                    "collision_ip_threshold": self._refinement_settings.collision_ip_threshold,
                    "collision_homogeneity_threshold": self._refinement_settings.collision_homogeneity_threshold,
                    "similarity_threshold": self._refinement_settings.similarity_threshold,
                    "min_sub_bundle_size": self._refinement_settings.min_sub_bundle_size,
                    "min_mibcs_improvement": self._refinement_settings.min_mibcs_improvement,
                },
            )
            logger.debug("Logged refinement run to session_refinement_log")
        except Exception as e:
            # Don't fail the main operation if logging fails
            logger.warning(f"Failed to log refinement run: {e}")

    def get_sessions_by_provider(
        self,
        bot_provider: str,
        limit: int = 100,
        min_confidence: str = "low",
    ) -> list[dict]:
        """
        Get sessions for a specific bot provider.

        Args:
            bot_provider: Bot provider to filter by
            limit: Maximum number of sessions to return
            min_confidence: Minimum confidence level ('low', 'medium', 'high')

        Returns:
            List of session dictionaries
        """
        self.initialize()

        confidence_order = {"high": 3, "medium": 2, "low": 1}
        min_level = confidence_order.get(min_confidence, 1)

        confidence_filter = []
        if min_level >= 3:
            confidence_filter = ["'high'"]
        elif min_level >= 2:
            confidence_filter = ["'high'", "'medium'"]
        else:
            confidence_filter = ["'high'", "'medium'", "'low'"]

        table_ref = self._get_table_ref()
        sql = f"""
            SELECT *
            FROM {table_ref}
            WHERE bot_provider = :bot_provider
              AND confidence_level IN ({', '.join(confidence_filter)})
            ORDER BY session_start_time DESC
            LIMIT {limit}
        """

        try:
            return self._backend.query(sql, {"bot_provider": bot_provider})
        except Exception as e:
            logger.warning(f"Failed to get sessions: {e}")
            return []

    # =========================================================================
    # Session URL Details (Flattened table for dashboarding)
    # =========================================================================

    URL_DETAILS_TABLE_NAME = "session_url_details"

    def _get_url_details_table_ref(self) -> str:
        """Get fully qualified table reference for session_url_details."""
        full_id = self._backend.get_full_table_id(self.URL_DETAILS_TABLE_NAME)
        if self._backend.backend_type == "bigquery":
            return f"`{full_id}`"
        return full_id

    def populate_url_details(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        delete_existing: bool = True,
    ) -> int:
        """
        Populate session_url_details table by flattening url_list from sessions.

        This creates one row per URL from the query_fanout_sessions table,
        which is essential for dashboarding where URL-level analysis is needed.

        Args:
            start_date: Start date (YYYY-MM-DD) to process, None for all data
            end_date: End date (YYYY-MM-DD) to process, None for all data
            delete_existing: Whether to delete existing data for date range first

        Returns:
            Number of rows inserted
        """
        self.initialize()

        sessions_table = self._get_table_ref()
        details_table = self._get_url_details_table_ref()

        # Build date filter
        date_conditions = []
        params: dict = {}
        if start_date:
            date_conditions.append("session_date >= :start_date")
            params["start_date"] = str(start_date)
        if end_date:
            date_conditions.append("session_date <= :end_date")
            params["end_date"] = str(end_date)

        date_filter = (
            f"WHERE {' AND '.join(date_conditions)}" if date_conditions else ""
        )

        # Delete existing data for date range if requested
        if delete_existing and date_conditions:
            delete_sql = f"DELETE FROM {details_table} {date_filter}"
            try:
                deleted = self._backend.execute(delete_sql, params)
                logger.info(f"Deleted {deleted:,} existing URL detail rows")
            except Exception as e:
                logger.warning(f"Failed to delete existing URL details: {e}")

        if self._backend.backend_type == "bigquery":
            return self._populate_url_details_bigquery(
                sessions_table, details_table, date_filter, params
            )
        else:
            return self._populate_url_details_sqlite(
                sessions_table, details_table, date_filter, params
            )

    def _populate_url_details_bigquery(
        self,
        sessions_table: str,
        details_table: str,
        date_filter: str,
        params: dict,
    ) -> int:
        """Populate URL details using BigQuery UNNEST for efficiency."""
        # Use UNNEST with offset to get URL position
        # Use CURRENT_TIMESTAMP() to avoid issues with param conversion
        sql = f"""
            INSERT INTO {details_table} (
                session_id, session_date, domain, url, url_position,
                bot_provider, bot_name, fanout_session_name, confidence_level,
                session_request_count, session_unique_urls, session_duration_ms,
                mean_cosine_similarity, min_cosine_similarity, max_cosine_similarity,
                session_start_time, session_end_time,
                window_ms, splitting_strategy, _created_at
            )
            SELECT
                s.session_id,
                s.session_date,
                s.domain,
                url,
                url_position,
                s.bot_provider,
                s.bot_name,
                s.fanout_session_name,
                s.confidence_level,
                s.request_count AS session_request_count,
                s.unique_urls AS session_unique_urls,
                s.duration_ms AS session_duration_ms,
                s.mean_cosine_similarity,
                s.min_cosine_similarity,
                s.max_cosine_similarity,
                s.session_start_time,
                s.session_end_time,
                s.window_ms,
                s.splitting_strategy,
                CURRENT_TIMESTAMP() AS _created_at
            FROM {sessions_table} s,
            UNNEST(JSON_EXTRACT_STRING_ARRAY(s.url_list)) AS url WITH OFFSET AS url_position
            {date_filter}
        """

        try:
            rows = self._backend.execute(sql, params)
            logger.info(f"Inserted {rows:,} URL detail rows (BigQuery)")
            return rows
        except Exception as e:
            logger.error(f"Failed to populate URL details: {e}")
            raise

    def _populate_url_details_sqlite(
        self,
        sessions_table: str,
        details_table: str,
        date_filter: str,
        params: dict,
    ) -> int:
        """Populate URL details for SQLite (requires row-by-row processing)."""
        # Fetch sessions
        sql = f"SELECT * FROM {sessions_table} {date_filter}"
        sessions = self._backend.query(sql, params)

        if not sessions:
            logger.info("No sessions found to process")
            return 0

        now = datetime.now(timezone.utc).isoformat()
        inserted = 0

        for session in sessions:
            try:
                # Parse URL list from JSON
                url_list = json.loads(session.get("url_list", "[]"))

                for position, url in enumerate(url_list, start=1):
                    insert_sql = f"""
                        INSERT INTO {details_table} (
                            session_id, session_date, domain, url, url_position,
                            bot_provider, bot_name, fanout_session_name, confidence_level,
                            session_request_count, session_unique_urls, session_duration_ms,
                            mean_cosine_similarity, min_cosine_similarity, max_cosine_similarity,
                            session_start_time, session_end_time,
                            window_ms, splitting_strategy, _created_at
                        ) VALUES (
                            :session_id, :session_date, :domain, :url, :url_position,
                            :bot_provider, :bot_name, :fanout_session_name, :confidence_level,
                            :session_request_count, :session_unique_urls, :session_duration_ms,
                            :mean_cosine_similarity, :min_cosine_similarity, :max_cosine_similarity,
                            :session_start_time, :session_end_time,
                            :window_ms, :splitting_strategy, :_created_at
                        )
                    """
                    self._backend.execute(
                        insert_sql,
                        {
                            "session_id": session["session_id"],
                            "session_date": session["session_date"],
                            "domain": session.get("domain"),
                            "url": url,
                            "url_position": position,
                            "bot_provider": session["bot_provider"],
                            "bot_name": session.get("bot_name"),
                            "fanout_session_name": session.get("fanout_session_name"),
                            "confidence_level": session["confidence_level"],
                            "session_request_count": session["request_count"],
                            "session_unique_urls": session["unique_urls"],
                            "session_duration_ms": session["duration_ms"],
                            "mean_cosine_similarity": session.get(
                                "mean_cosine_similarity"
                            ),
                            "min_cosine_similarity": session.get(
                                "min_cosine_similarity"
                            ),
                            "max_cosine_similarity": session.get(
                                "max_cosine_similarity"
                            ),
                            "session_start_time": session["session_start_time"],
                            "session_end_time": session["session_end_time"],
                            "window_ms": session["window_ms"],
                            "splitting_strategy": session.get("splitting_strategy"),
                            "_created_at": now,
                        },
                    )
                    inserted += 1

            except Exception as e:
                logger.warning(
                    f"Failed to process session {session.get('session_id')}: {e}"
                )

        logger.info(f"Inserted {inserted:,} URL detail rows (SQLite)")
        return inserted

    def delete_url_details(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """
        Delete URL detail records.

        Args:
            start_date: Start date (YYYY-MM-DD) to delete, None for all
            end_date: End date (YYYY-MM-DD) to delete, None for all

        Returns:
            Number of rows deleted
        """
        self.initialize()
        details_table = self._get_url_details_table_ref()

        # Build date filter
        conditions = []
        params: dict = {}
        if start_date:
            conditions.append("session_date >= :start_date")
            params["start_date"] = str(start_date)
        if end_date:
            conditions.append("session_date <= :end_date")
            params["end_date"] = str(end_date)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"DELETE FROM {details_table} {where_clause}"

        try:
            return self._backend.execute(sql, params)
        except Exception as e:
            logger.warning(f"Failed to delete URL details: {e}")
            return 0

    def get_url_details_summary(self) -> dict:
        """
        Get summary statistics for session_url_details table.

        Returns:
            Dictionary with summary statistics
        """
        self.initialize()
        details_table = self._get_url_details_table_ref()

        sql = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT session_id) as unique_sessions,
                COUNT(DISTINCT url) as unique_urls,
                COUNT(DISTINCT bot_provider) as unique_providers,
                MIN(session_date) as earliest_date,
                MAX(session_date) as latest_date
            FROM {details_table}
        """

        try:
            results = self._backend.query(sql)
            if results:
                return results[0]
            return {}
        except Exception as e:
            logger.warning(f"Failed to get URL details summary: {e}")
            return {"error": str(e)}
