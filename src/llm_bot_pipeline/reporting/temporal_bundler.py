"""
Temporal bundling for query fan-out session grouping.

Provides a focused reporting-layer interface for grouping timestamped
requests into time-based sessions (bundles). Wraps the research-level
TemporalAnalyzer with a clean API oriented toward session creation.

Used by SessionAggregator (and future refactored session components)
as the Stage 1 temporal bundling step.
"""

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

import pandas as pd

from ..research.temporal_analysis import (
    Bundle,
    TemporalAnalyzer,
    compute_bundle_statistics,
)

logger = logging.getLogger(__name__)


@dataclass
class BundleResult:
    """Result of a temporal bundling operation."""

    bundles: list[Bundle]
    total_requests: int
    window_ms: float
    providers_processed: list[str]

    @property
    def bundle_count(self) -> int:
        return len(self.bundles)

    @property
    def mean_bundle_size(self) -> float:
        if not self.bundles:
            return 0.0
        return self.total_requests / len(self.bundles)

    def statistics(self) -> dict:
        """Compute detailed bundle statistics."""
        return compute_bundle_statistics(self.bundles)


class TemporalBundler:
    """
    Groups timestamped requests into time-based session bundles.

    Applies greedy temporal clustering: requests within ``window_ms``
    of the first request in a bundle are grouped together. When a
    request exceeds the window, a new bundle starts.

    Bundles are created per ``group_by`` column (default: bot_provider)
    so that different bot providers are never mixed in the same session.
    """

    def __init__(
        self,
        window_ms: float,
        timestamp_col: str = "datetime",
        url_col: str = "url",
        group_by: str = "bot_provider",
    ):
        """
        Args:
            window_ms: Maximum time window for bundling in milliseconds.
            timestamp_col: Name of the timestamp column in input DataFrames.
            url_col: Name of the URL column in input DataFrames.
            group_by: Column to partition requests by before bundling.
        """
        if window_ms <= 0:
            raise ValueError("window_ms must be positive")

        self.window_ms = window_ms
        self.timestamp_col = timestamp_col
        self.url_col = url_col
        self.group_by = group_by

    def bundle_by_time(self, df: pd.DataFrame) -> BundleResult:
        """
        Group requests into temporal bundles.

        This is the primary entry point. It sorts requests by timestamp,
        partitions by ``group_by``, and clusters each partition using a
        greedy sliding-window approach.

        Args:
            df: DataFrame containing at least ``timestamp_col``, ``url_col``,
                and ``group_by`` columns.

        Returns:
            BundleResult with the created bundles and summary metadata.

        Raises:
            KeyError: If required columns are missing from the DataFrame.
            ValueError: If the DataFrame is None.
        """
        self._validate_dataframe(df)

        if df.empty:
            logger.info("Empty DataFrame provided, returning empty result")
            return BundleResult(
                bundles=[],
                total_requests=0,
                window_ms=self.window_ms,
                providers_processed=[],
            )

        analyzer = TemporalAnalyzer(
            timestamp_col=self.timestamp_col,
            url_col=self.url_col,
            group_by=self.group_by,
        )
        analyzer.load_data(df)
        bundles = analyzer.create_bundles(self.window_ms)

        providers = (
            sorted(df[self.group_by].unique()) if self.group_by in df.columns else []
        )

        total_requests = sum(b.request_count for b in bundles)

        logger.info(
            "Bundled %d requests into %d bundles (window=%.0fms, providers=%s)",
            total_requests,
            len(bundles),
            self.window_ms,
            providers,
        )

        return BundleResult(
            bundles=bundles,
            total_requests=total_requests,
            window_ms=self.window_ms,
            providers_processed=list(providers),
        )

    def group_requests(
        self,
        df: pd.DataFrame,
        session_timeout_ms: Optional[float] = None,
    ) -> BundleResult:
        """
        Group requests with an optional custom session timeout.

        Convenience wrapper around :meth:`bundle_by_time` that allows
        overriding the default ``window_ms`` for one-off calls.

        Args:
            df: DataFrame of requests.
            session_timeout_ms: Override window for this call only.
                If None, uses the instance's ``window_ms``.

        Returns:
            BundleResult with the created bundles.
        """
        if session_timeout_ms is not None:
            if session_timeout_ms <= 0:
                raise ValueError("session_timeout_ms must be positive")
            original = self.window_ms
            self.window_ms = session_timeout_ms
            try:
                return self.bundle_by_time(df)
            finally:
                self.window_ms = original

        return self.bundle_by_time(df)

    def is_session_timeout(
        self,
        gap_ms: float,
        timeout_ms: Optional[float] = None,
    ) -> bool:
        """
        Check whether a time gap exceeds the session timeout.

        Useful for point-checks when processing records sequentially.

        Args:
            gap_ms: Time gap between two consecutive requests (ms).
            timeout_ms: Custom timeout threshold; defaults to ``window_ms``.

        Returns:
            True if the gap exceeds the timeout (new session should start).
        """
        threshold = timeout_ms if timeout_ms is not None else self.window_ms
        return gap_ms > threshold

    def estimate_session_count(self, df: pd.DataFrame) -> int:
        """
        Cheaply estimate the number of sessions without full bundling.

        Counts the number of inter-request gaps exceeding ``window_ms``.
        The estimate is ``gaps + number_of_groups`` (each provider group
        contributes at least one session).

        Args:
            df: DataFrame of requests.

        Returns:
            Estimated number of sessions.
        """
        self._validate_dataframe(df)

        if df.empty:
            return 0

        df = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df[self.timestamp_col]):
            df[self.timestamp_col] = pd.to_datetime(
                df[self.timestamp_col], format="ISO8601"
            )

        window_td = timedelta(milliseconds=self.window_ms)
        session_count = 0

        groups = (
            df.groupby(self.group_by) if self.group_by in df.columns else [(None, df)]
        )

        for _, group_df in groups:
            if group_df.empty:
                continue
            sorted_ts = group_df[self.timestamp_col].sort_values()
            gaps = sorted_ts.diff().dropna()
            timeouts = (gaps > window_td).sum()
            session_count += timeouts + 1  # +1 for the first session in group

        return int(session_count)

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """Raise early if required columns are missing."""
        if df is None:
            raise ValueError("DataFrame must not be None")

        required = {self.timestamp_col, self.url_col}
        missing = required - set(df.columns)
        if missing:
            raise KeyError(f"Missing required columns: {missing}")
