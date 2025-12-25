"""
Temporal analysis for query fan-out bundling.

Provides tools for analyzing inter-request time deltas and identifying
natural clustering boundaries in LLM bot request patterns.
"""

import logging
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from ..storage.sqlite_backend import VALID_TABLES

logger = logging.getLogger(__name__)


@dataclass
class DeltaStats:
    """Statistics for inter-request time deltas."""

    count: int
    mean_ms: float
    median_ms: float
    std_ms: float
    min_ms: float
    max_ms: float
    percentiles: dict[str, float]  # p50, p75, p90, p95, p99


@dataclass
class Bundle:
    """A temporal bundle of related requests."""

    bundle_id: str
    start_time: datetime
    end_time: datetime
    request_count: int
    bot_provider: str
    urls: list[str] = field(default_factory=list)
    request_indices: list[int] = field(default_factory=list)

    @property
    def duration_ms(self) -> float:
        """Duration of bundle in milliseconds."""
        return (self.end_time - self.start_time).total_seconds() * 1000


def compute_inter_request_deltas(
    df: pd.DataFrame,
    timestamp_col: str = "request_timestamp",
    group_by: Optional[str] = "bot_provider",
) -> pd.DataFrame:
    """
    Compute inter-request time deltas for consecutive requests.

    Args:
        df: DataFrame with request data
        timestamp_col: Name of timestamp column (default: request_timestamp)
        group_by: Column to group by (e.g., 'bot_provider'), or None for global

    Returns:
        DataFrame with added 'delta_ms' column containing time since previous request
    """
    df = df.copy()

    # Ensure timestamp is datetime
    if df[timestamp_col].dtype == "object":
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], format="ISO8601")

    # Sort by timestamp
    df = df.sort_values(timestamp_col)

    if group_by and group_by in df.columns:
        # Compute delta within each group
        df["delta_ms"] = (
            df.groupby(group_by)[timestamp_col].diff().dt.total_seconds() * 1000
        )
    else:
        # Compute global delta
        df["delta_ms"] = df[timestamp_col].diff().dt.total_seconds() * 1000

    return df


def compute_delta_stats(
    df: pd.DataFrame,
    delta_col: str = "delta_ms",
    exclude_first: bool = True,
) -> DeltaStats:
    """
    Compute statistics for inter-request time deltas.

    Args:
        df: DataFrame with delta column
        delta_col: Name of delta column
        exclude_first: Whether to exclude NaN values (first request in each group)

    Returns:
        DeltaStats with summary statistics
    """
    deltas = df[delta_col].dropna() if exclude_first else df[delta_col]

    if len(deltas) == 0:
        return DeltaStats(
            count=0,
            mean_ms=0,
            median_ms=0,
            std_ms=0,
            min_ms=0,
            max_ms=0,
            percentiles={},
        )

    percentile_values = {
        "p50": float(np.percentile(deltas, 50)),
        "p75": float(np.percentile(deltas, 75)),
        "p90": float(np.percentile(deltas, 90)),
        "p95": float(np.percentile(deltas, 95)),
        "p99": float(np.percentile(deltas, 99)),
    }

    return DeltaStats(
        count=len(deltas),
        mean_ms=float(deltas.mean()),
        median_ms=float(deltas.median()),
        std_ms=float(deltas.std()),
        min_ms=float(deltas.min()),
        max_ms=float(deltas.max()),
        percentiles=percentile_values,
    )


def find_natural_gaps(
    deltas: np.ndarray,
    method: str = "percentile",
    **kwargs,
) -> list[float]:
    """
    Find natural gap thresholds in delta distribution.

    Args:
        deltas: Array of time deltas in milliseconds
        method: Detection method ('percentile', 'histogram', 'elbow')
        **kwargs: Method-specific parameters

    Returns:
        List of candidate gap thresholds in milliseconds
    """
    deltas = deltas[~np.isnan(deltas)]

    if len(deltas) == 0:
        return []

    if method == "percentile":
        # Return key percentiles as candidate thresholds
        percentiles = kwargs.get("percentiles", [75, 90, 95, 99])
        return [float(np.percentile(deltas, p)) for p in percentiles]

    elif method == "histogram":
        # Find valleys in histogram (gaps between modes)
        n_bins = kwargs.get("n_bins", 50)
        hist, bin_edges = np.histogram(np.log10(deltas + 1), bins=n_bins)

        # Find local minima
        gaps = []
        for i in range(1, len(hist) - 1):
            if hist[i] < hist[i - 1] and hist[i] < hist[i + 1]:
                # Convert back from log scale
                threshold = 10 ** bin_edges[i] - 1
                gaps.append(float(threshold))

        return sorted(gaps)

    elif method == "elbow":
        # Use elbow/knee detection on sorted deltas
        sorted_deltas = np.sort(deltas)
        n = len(sorted_deltas)

        # Compute second derivative to find inflection points
        if n < 10:
            return [float(np.percentile(deltas, 90))]

        # Subsample for efficiency
        sample_indices = np.linspace(0, n - 1, min(n, 1000), dtype=int)
        sample = sorted_deltas[sample_indices]

        # Find point of maximum curvature
        diff1 = np.diff(sample)
        diff2 = np.diff(diff1)

        # Find largest jumps
        jump_indices = np.argsort(diff1)[-5:]  # Top 5 jumps
        gaps = [float(sample[i]) for i in jump_indices if i < len(sample)]

        return sorted(gaps)

    else:
        raise ValueError(f"Unknown method: {method}")


def create_temporal_bundles(
    df: pd.DataFrame,
    window_ms: float,
    timestamp_col: str = "request_timestamp",
    url_col: str = "request_uri",
    group_by: Optional[str] = "bot_provider",
) -> list[Bundle]:
    """
    Create temporal bundles using greedy clustering.

    Args:
        df: DataFrame with request data
        window_ms: Maximum time window in milliseconds
        timestamp_col: Name of timestamp column (default: request_timestamp)
        url_col: Name of URL column (default: request_uri)
        group_by: Column to group by (e.g., 'bot_provider'), or None for global

    Returns:
        List of Bundle objects
    """
    df = df.copy()

    # Ensure timestamp is datetime
    if df[timestamp_col].dtype == "object":
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], format="ISO8601")

    # Sort by timestamp
    df = df.sort_values(timestamp_col).reset_index(drop=True)

    bundles = []
    window_td = timedelta(milliseconds=window_ms)

    if group_by and group_by in df.columns:
        # Process each group separately
        for group_name, group_df in df.groupby(group_by):
            group_bundles = _create_bundles_for_group(
                group_df,
                window_td,
                timestamp_col,
                url_col,
                str(group_name),
            )
            bundles.extend(group_bundles)
    else:
        # Process all data as one group
        bundles = _create_bundles_for_group(
            df,
            window_td,
            timestamp_col,
            url_col,
            "all",
        )

    return bundles


def _create_bundles_for_group(
    df: pd.DataFrame,
    window_td: timedelta,
    timestamp_col: str,
    url_col: str,
    bot_provider: str,
) -> list[Bundle]:
    """Create bundles for a single group of requests."""
    if len(df) == 0:
        return []

    df = df.sort_values(timestamp_col).reset_index(drop=True)

    bundles = []
    current_bundle_start = df.iloc[0][timestamp_col]
    current_bundle_indices = [0]
    current_bundle_urls = [df.iloc[0][url_col]]

    for i in range(1, len(df)):
        row = df.iloc[i]
        timestamp = row[timestamp_col]

        if timestamp - current_bundle_start <= window_td:
            # Add to current bundle
            current_bundle_indices.append(i)
            current_bundle_urls.append(row[url_col])
        else:
            # Finalize current bundle and start new one
            bundle = Bundle(
                bundle_id=str(uuid.uuid4()),
                start_time=current_bundle_start,
                end_time=df.iloc[current_bundle_indices[-1]][timestamp_col],
                request_count=len(current_bundle_indices),
                bot_provider=bot_provider,
                urls=current_bundle_urls,
                request_indices=current_bundle_indices,
            )
            bundles.append(bundle)

            # Start new bundle
            current_bundle_start = timestamp
            current_bundle_indices = [i]
            current_bundle_urls = [row[url_col]]

    # Don't forget the last bundle
    if current_bundle_indices:
        bundle = Bundle(
            bundle_id=str(uuid.uuid4()),
            start_time=current_bundle_start,
            end_time=df.iloc[current_bundle_indices[-1]][timestamp_col],
            request_count=len(current_bundle_indices),
            bot_provider=bot_provider,
            urls=current_bundle_urls,
            request_indices=current_bundle_indices,
        )
        bundles.append(bundle)

    return bundles


def compute_bundle_statistics(bundles: list[Bundle]) -> dict:
    """
    Compute summary statistics for a list of bundles.

    Args:
        bundles: List of Bundle objects

    Returns:
        Dictionary with bundle statistics
    """
    if not bundles:
        return {
            "total_bundles": 0,
            "total_requests": 0,
            "mean_bundle_size": 0,
            "median_bundle_size": 0,
            "singleton_count": 0,
            "singleton_rate": 0,
            "giant_count": 0,
            "giant_rate": 0,
            "size_distribution": {},
        }

    sizes = [b.request_count for b in bundles]
    # Use unique URLs for singleton/giant classification (not raw request count)
    unique_url_counts = [len(set(b.urls)) for b in bundles]
    total_bundles = len(bundles)

    # Singleton = session with only 1 unique URL
    singleton_count = sum(1 for u in unique_url_counts if u == 1)
    # Giant = session with >10 unique URLs
    giant_count = sum(1 for u in unique_url_counts if u > 10)

    # Size distribution
    size_dist = {}
    for s in sizes:
        bucket = str(s) if s <= 5 else "6-10" if s <= 10 else ">10"
        size_dist[bucket] = size_dist.get(bucket, 0) + 1

    return {
        "total_bundles": total_bundles,
        "total_requests": sum(sizes),
        "mean_bundle_size": float(np.mean(sizes)),
        "median_bundle_size": float(np.median(sizes)),
        "min_bundle_size": min(sizes),
        "max_bundle_size": max(sizes),
        "singleton_count": singleton_count,
        "singleton_rate": singleton_count / total_bundles,
        "giant_count": giant_count,
        "giant_rate": giant_count / total_bundles,
        "size_distribution": size_dist,
    }


class TemporalAnalyzer:
    """
    High-level interface for temporal analysis of request patterns.

    Provides methods for:
    - Loading and preprocessing request data
    - Computing inter-request deltas
    - Analyzing delta distributions
    - Creating temporal bundles
    """

    def __init__(
        self,
        timestamp_col: str = "request_timestamp",
        url_col: str = "request_uri",
        group_by: str = "bot_provider",
    ):
        """
        Initialize temporal analyzer.

        Args:
            timestamp_col: Name of timestamp column (default: request_timestamp)
            url_col: Name of URL column (default: request_uri)
            group_by: Column to group by for per-provider analysis
        """
        self.timestamp_col = timestamp_col
        self.url_col = url_col
        self.group_by = group_by
        self._df: Optional[pd.DataFrame] = None
        self._delta_stats: Optional[dict[str, DeltaStats]] = None

    def load_data(self, df: pd.DataFrame) -> "TemporalAnalyzer":
        """
        Load request data for analysis.

        Args:
            df: DataFrame with request data

        Returns:
            self for method chaining
        """
        self._df = df.copy()

        # Ensure timestamp is datetime
        if self._df[self.timestamp_col].dtype == "object":
            self._df[self.timestamp_col] = pd.to_datetime(
                self._df[self.timestamp_col], format="ISO8601"
            )

        # Compute deltas
        self._df = compute_inter_request_deltas(
            self._df,
            timestamp_col=self.timestamp_col,
            group_by=self.group_by,
        )

        logger.info(f"Loaded {len(self._df)} requests for temporal analysis")
        return self

    def load_from_csv(self, path: str) -> "TemporalAnalyzer":
        """
        Load request data from CSV file.

        Args:
            path: Path to CSV file

        Returns:
            self for method chaining
        """
        df = pd.read_csv(path)
        return self.load_data(df)

    def load_from_sqlite(
        self,
        db_path: str,
        table_name: str = "bot_requests_daily",
    ) -> "TemporalAnalyzer":
        """
        Load request data from SQLite database.

        Args:
            db_path: Path to SQLite database file
            table_name: Table to read from (default: bot_requests_daily)

        Returns:
            self for method chaining

        Raises:
            FileNotFoundError: If database file doesn't exist
            ValueError: If table name is not in allowed list
        """
        path = Path(db_path)
        if not path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        # Validate table name to prevent SQL injection
        if table_name not in VALID_TABLES:
            raise ValueError(
                f"Invalid table name: '{table_name}'. "
                f"Must be one of: {sorted(VALID_TABLES)}"
            )

        query = f"SELECT * FROM {table_name}"
        with sqlite3.connect(str(path)) as conn:
            df = pd.read_sql_query(query, conn)

        logger.info(f"Loaded {len(df):,} records from {table_name}")
        return self.load_data(df)

    def get_delta_stats(self, by_provider: bool = True) -> dict[str, DeltaStats]:
        """
        Get delta statistics, optionally by provider.

        Args:
            by_provider: Whether to compute stats per provider

        Returns:
            Dictionary mapping provider name (or 'all') to DeltaStats
        """
        if self._df is None:
            raise ValueError("No data loaded. Call load_data() first.")

        stats = {}

        if by_provider and self.group_by in self._df.columns:
            for provider, group_df in self._df.groupby(self.group_by):
                stats[str(provider)] = compute_delta_stats(group_df)
        else:
            stats["all"] = compute_delta_stats(self._df)

        self._delta_stats = stats
        return stats

    def find_candidate_windows(
        self,
        method: str = "percentile",
        **kwargs,
    ) -> list[float]:
        """
        Find candidate bundling windows based on delta distribution.

        Args:
            method: Detection method ('percentile', 'histogram', 'elbow')
            **kwargs: Method-specific parameters

        Returns:
            List of candidate window thresholds in milliseconds
        """
        if self._df is None:
            raise ValueError("No data loaded. Call load_data() first.")

        deltas = self._df["delta_ms"].dropna().values
        return find_natural_gaps(deltas, method=method, **kwargs)

    def create_bundles(self, window_ms: float) -> list[Bundle]:
        """
        Create temporal bundles using specified window.

        Args:
            window_ms: Bundling window in milliseconds

        Returns:
            List of Bundle objects
        """
        if self._df is None:
            raise ValueError("No data loaded. Call load_data() first.")

        return create_temporal_bundles(
            self._df,
            window_ms=window_ms,
            timestamp_col=self.timestamp_col,
            url_col=self.url_col,
            group_by=self.group_by,
        )

    def get_bundle_stats(self, window_ms: float) -> dict:
        """
        Get bundle statistics for a given window.

        Args:
            window_ms: Bundling window in milliseconds

        Returns:
            Dictionary with bundle statistics
        """
        bundles = self.create_bundles(window_ms)
        return compute_bundle_statistics(bundles)
