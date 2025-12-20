"""
Query fan-out session aggregations for reporting.

Creates and stores query fan-out sessions by bundling temporally-clustered
requests that likely originated from a single user question to an LLM.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from ..research.semantic_embeddings import URLEmbedder, get_confidence_level
from ..research.temporal_analysis import Bundle, TemporalAnalyzer
from ..storage import StorageBackend, get_backend
from ..utils.url_utils import derive_session_name

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
    _created_at: str


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


class SessionAggregator:
    """
    Creates query fan-out session aggregations.

    Bundles temporally-clustered LLM bot requests into sessions
    and stores them with semantic similarity metrics.
    """

    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        backend_type: str = "sqlite",
        db_path: Optional[Path] = None,
        embedding_method: str = "tfidf",
    ):
        """
        Initialize session aggregator.

        Args:
            backend: Pre-initialized StorageBackend (optional)
            backend_type: Backend type if creating new ('sqlite')
            db_path: Path to SQLite database (for sqlite backend)
            embedding_method: Method for URL embeddings ('tfidf' or 'transformer')
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
        self._initialized = False

        logger.info(
            f"SessionAggregator initialized with {self._backend.backend_type} backend"
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

    def __enter__(self) -> "SessionAggregator":
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def create_sessions_from_dataframe(
        self,
        df: pd.DataFrame,
        window_ms: float,
        timestamp_col: str = "datetime",
        url_col: str = "url",
        group_by: str = "bot_provider",
        bot_name_col: Optional[str] = "bot_name",
    ) -> SessionAggregationResult:
        """
        Create sessions from a DataFrame of requests.

        Args:
            df: DataFrame with request data
            window_ms: Time window for bundling in milliseconds
            timestamp_col: Name of timestamp column
            url_col: Name of URL column
            group_by: Column to group by (e.g., 'bot_provider')
            bot_name_col: Column with bot name (optional)

        Returns:
            SessionAggregationResult with statistics
        """
        self.initialize()
        started_at = datetime.now().astimezone()
        logger.info(
            f"Creating sessions with {window_ms}ms window from {len(df)} requests"
        )

        try:
            # Create temporal bundles
            analyzer = TemporalAnalyzer(
                timestamp_col=timestamp_col,
                url_col=url_col,
                group_by=group_by,
            )
            analyzer.load_data(df)
            bundles = analyzer.create_bundles(window_ms)

            logger.info(f"Created {len(bundles)} bundles")

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
                        datetime.now().astimezone() - started_at
                    ).total_seconds(),
                )

            # Fit embedder on all URLs
            all_urls = []
            for bundle in bundles:
                all_urls.extend(bundle.urls)
            if all_urls:
                self._embedder.fit(all_urls)

            # Convert bundles to session records
            records = self._bundles_to_records(
                bundles,
                window_ms,
                df,
                group_by,
                bot_name_col,
            )

            # Insert records
            sessions_created = self._insert_session_records(records)

            # Calculate statistics
            confidence_counts = {"high": 0, "medium": 0, "low": 0}
            for record in records:
                confidence_counts[record.confidence_level] += 1

            total_requests = sum(r.request_count for r in records)
            mean_size = total_requests / len(records) if records else 0

            duration = (datetime.now().astimezone() - started_at).total_seconds()

            logger.info(
                f"Created {sessions_created} sessions in {duration:.1f}s "
                f"(high: {confidence_counts['high']}, "
                f"medium: {confidence_counts['medium']}, "
                f"low: {confidence_counts['low']})"
            )

            return SessionAggregationResult(
                success=True,
                sessions_created=sessions_created,
                total_requests_bundled=total_requests,
                window_ms=window_ms,
                mean_session_size=mean_size,
                high_confidence_count=confidence_counts["high"],
                medium_confidence_count=confidence_counts["medium"],
                low_confidence_count=confidence_counts["low"],
                duration_seconds=duration,
            )

        except Exception as e:
            logger.exception(f"Failed to create sessions: {e}")
            duration = (datetime.now().astimezone() - started_at).total_seconds()
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
        group_by: str,
        bot_name_col: Optional[str],
    ) -> list[SessionRecord]:
        """Convert Bundle objects to SessionRecord objects."""
        records = []
        now = datetime.now().astimezone().isoformat()

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

            # Get bot name if available
            bot_name = None
            if bot_name_col and bundle.request_indices:
                try:
                    first_idx = bundle.request_indices[0]
                    if first_idx < len(df) and bot_name_col in df.columns:
                        bot_name = df.iloc[first_idx][bot_name_col]
                except (IndexError, KeyError):
                    pass

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

            record = SessionRecord(
                session_id=bundle.bundle_id,
                session_date=bundle.start_time.date().isoformat(),
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
                _created_at=now,
            )
            records.append(record)

        return records

    def _insert_session_records(self, records: list[SessionRecord]) -> int:
        """Insert session records into database."""
        if not records:
            return 0

        sql = """
            INSERT INTO query_fanout_sessions (
                session_id, session_date, session_start_time, session_end_time,
                duration_ms, bot_provider, bot_name, request_count, unique_urls,
                mean_cosine_similarity, min_cosine_similarity, max_cosine_similarity,
                confidence_level, fanout_session_name, url_list, window_ms, _created_at
            ) VALUES (
                :session_id, :session_date, :session_start_time, :session_end_time,
                :duration_ms, :bot_provider, :bot_name, :request_count, :unique_urls,
                :mean_cosine_similarity, :min_cosine_similarity, :max_cosine_similarity,
                :confidence_level, :fanout_session_name, :url_list, :window_ms, :_created_at
            )
        """

        inserted = 0
        for record in records:
            try:
                params = {
                    "session_id": record.session_id,
                    "session_date": record.session_date,
                    "session_start_time": record.session_start_time,
                    "session_end_time": record.session_end_time,
                    "duration_ms": record.duration_ms,
                    "bot_provider": record.bot_provider,
                    "bot_name": record.bot_name,
                    "request_count": record.request_count,
                    "unique_urls": record.unique_urls,
                    "mean_cosine_similarity": record.mean_cosine_similarity,
                    "min_cosine_similarity": record.min_cosine_similarity,
                    "max_cosine_similarity": record.max_cosine_similarity,
                    "confidence_level": record.confidence_level,
                    "fanout_session_name": record.fanout_session_name,
                    "url_list": record.url_list,
                    "window_ms": record.window_ms,
                    "_created_at": record._created_at,
                }
                self._backend.execute(sql, params)
                inserted += 1
            except Exception as e:
                logger.warning(f"Failed to insert session {record.session_id}: {e}")

        return inserted

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

        conditions = []
        params = {}

        if session_date:
            conditions.append("session_date = :session_date")
            params["session_date"] = session_date

        if bot_provider:
            conditions.append("bot_provider = :bot_provider")
            params["bot_provider"] = bot_provider

        if conditions:
            where_clause = " AND ".join(conditions)
            sql = f"DELETE FROM query_fanout_sessions WHERE {where_clause}"
        else:
            sql = "DELETE FROM query_fanout_sessions"

        try:
            return self._backend.execute(sql, params)
        except Exception as e:
            logger.warning(f"Failed to delete sessions: {e}")
            return 0

    def get_session_summary(self) -> dict:
        """
        Get summary statistics for all sessions.

        Returns:
            Dictionary with session statistics
        """
        self.initialize()

        sql = """
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
            FROM query_fanout_sessions
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

        sql = f"""
            SELECT *
            FROM query_fanout_sessions
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
