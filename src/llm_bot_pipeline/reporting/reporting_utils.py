"""
Reporting utilities for database setup and maintenance.

Provides utility functions for:
- Database setup and initialization
- View recreation
- Data integrity validation
- Dashboard metrics retrieval
"""

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

from ..storage import StorageBackend, get_backend
from ..storage.sqlite_schemas import VIEW_DEFINITIONS

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of data integrity validation."""

    is_valid: bool
    checks_passed: int
    checks_failed: int
    errors: list[str]
    warnings: list[str]


@dataclass
class DashboardMetrics:
    """Collection of dashboard metrics."""

    total_sessions: int
    avg_urls_per_session: float
    singleton_rate: float
    mean_mibcs_multi_url: Optional[float]
    high_confidence_rate: float
    url_distribution: dict[str, int]
    top_bots: list[dict[str, Any]]
    period: str


class ReportingUtilities:
    """
    Utility class for reporting database setup and maintenance.

    Provides functions for database initialization, view management,
    data validation, and metrics retrieval.
    """

    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        backend_type: str = "sqlite",
        db_path: Optional[Path] = None,
    ):
        """
        Initialize reporting utilities.

        Args:
            backend: Pre-initialized StorageBackend (optional)
            backend_type: Backend type if creating new ('sqlite')
            db_path: Path to SQLite database (for sqlite backend)
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

        self._initialized = False
        logger.info(
            f"ReportingUtilities initialized with {self._backend.backend_type} backend"
        )

    def initialize(self) -> None:
        """Initialize the backend (create tables and views)."""
        if not self._initialized:
            self._backend.initialize()
            self._initialized = True

    def close(self) -> None:
        """Close the backend connection."""
        if self._owns_backend:
            self._backend.close()

    def __enter__(self) -> "ReportingUtilities":
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    # =========================================================================
    # Setup Functions
    # =========================================================================

    def setup_reporting_tables(self) -> dict[str, Any]:
        """
        Create all reporting tables and views.

        This is a convenience wrapper around backend.initialize()
        that also returns setup status.

        Returns:
            Dictionary with setup status and created objects
        """
        self.initialize()

        # Verify tables were created
        tables = self._backend.query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        views = self._backend.query(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        )

        return {
            "success": True,
            "tables_created": [t["name"] for t in tables],
            "views_created": [v["name"] for v in views],
            "table_count": len(tables),
            "view_count": len(views),
        }

    def recreate_views(self) -> dict[str, Any]:
        """
        Drop and recreate all reporting views.

        Useful when view definitions have changed and need to be updated.

        Returns:
            Dictionary with recreation status
        """
        self.initialize()

        view_names = [
            "v_session_url_distribution",
            "v_session_singleton_binary",
            "v_bot_volume",
            "v_top_session_topics",
            "v_daily_kpis",
            "v_category_comparison",
            "v_url_cooccurrence",
        ]

        dropped = []
        created = []
        errors = []

        # Drop existing views
        for view_name in view_names:
            try:
                self._backend.execute(f"DROP VIEW IF EXISTS {view_name}", {})
                dropped.append(view_name)
            except Exception as e:
                errors.append(f"Failed to drop {view_name}: {e}")

        # Recreate views
        for view_sql in VIEW_DEFINITIONS:
            try:
                self._backend.execute(view_sql, {})
                # Extract view name from SQL
                view_name = view_sql.split("IF NOT EXISTS")[1].split("AS")[0].strip()
                created.append(view_name)
            except Exception as e:
                errors.append(f"Failed to create view: {e}")

        return {
            "success": len(errors) == 0,
            "views_dropped": dropped,
            "views_created": created,
            "errors": errors,
        }

    # =========================================================================
    # Validation Functions
    # =========================================================================

    def validate_data_integrity(self) -> ValidationResult:
        """
        Check data integrity including FK constraints and consistency.

        Performs the following checks:
        - session_url_details references valid session_ids
        - unique_urls count matches url_list length
        - confidence_level values are valid
        - Required NOT NULL fields are populated

        Returns:
            ValidationResult with check details
        """
        self.initialize()

        errors = []
        warnings = []
        checks_passed = 0
        checks_failed = 0

        # Check 1: session_url_details FK integrity
        orphaned_details = self._backend.query("""
            SELECT COUNT(*) as count
            FROM session_url_details sud
            LEFT JOIN query_fanout_sessions qfs ON sud.session_id = qfs.session_id
            WHERE qfs.session_id IS NULL
        """)
        if orphaned_details and orphaned_details[0]["count"] > 0:
            errors.append(
                f"Found {orphaned_details[0]['count']} session_url_details "
                "rows with invalid session_id"
            )
            checks_failed += 1
        else:
            checks_passed += 1

        # Check 2: unique_urls consistency
        inconsistent_counts = self._backend.query("""
            SELECT COUNT(*) as count
            FROM query_fanout_sessions
            WHERE unique_urls != json_array_length(url_list)
        """)
        if inconsistent_counts and inconsistent_counts[0]["count"] > 0:
            warnings.append(
                f"Found {inconsistent_counts[0]['count']} sessions where "
                "unique_urls doesn't match url_list length"
            )

        # Check 3: confidence_level values
        invalid_confidence = self._backend.query("""
            SELECT COUNT(*) as count
            FROM query_fanout_sessions
            WHERE confidence_level NOT IN ('high', 'medium', 'low')
        """)
        if invalid_confidence and invalid_confidence[0]["count"] > 0:
            errors.append(
                f"Found {invalid_confidence[0]['count']} sessions "
                "with invalid confidence_level"
            )
            checks_failed += 1
        else:
            checks_passed += 1

        # Check 4: Required fields populated in query_fanout_sessions
        null_required = self._backend.query("""
            SELECT COUNT(*) as count
            FROM query_fanout_sessions
            WHERE session_id IS NULL
               OR session_date IS NULL
               OR bot_provider IS NULL
               OR confidence_level IS NULL
        """)
        if null_required and null_required[0]["count"] > 0:
            errors.append(
                f"Found {null_required[0]['count']} sessions "
                "with NULL required fields"
            )
            checks_failed += 1
        else:
            checks_passed += 1

        # Check 5: session_url_details required fields
        null_details = self._backend.query("""
            SELECT COUNT(*) as count
            FROM session_url_details
            WHERE session_id IS NULL
               OR url IS NULL
               OR bot_provider IS NULL
               OR confidence_level IS NULL
        """)
        if null_details and null_details[0]["count"] > 0:
            errors.append(
                f"Found {null_details[0]['count']} URL details "
                "with NULL required fields"
            )
            checks_failed += 1
        else:
            checks_passed += 1

        # Check 6: Views are queryable
        view_names = [
            "v_session_url_distribution",
            "v_daily_kpis",
            "v_bot_volume",
            "v_url_cooccurrence",
        ]
        for view_name in view_names:
            try:
                self._backend.query(f"SELECT * FROM {view_name} LIMIT 1")
                checks_passed += 1
            except Exception as e:
                errors.append(f"View {view_name} is not queryable: {e}")
                checks_failed += 1

        return ValidationResult(
            is_valid=len(errors) == 0,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            errors=errors,
            warnings=warnings,
        )

    # =========================================================================
    # Dashboard Metrics Functions
    # =========================================================================

    def get_dashboard_metrics(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> DashboardMetrics:
        """
        Get all dashboard metrics in a single call.

        Executes queries to populate dashboard scorecards.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            DashboardMetrics with all KPIs
        """
        self.initialize()

        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        period = f"{start_date.isoformat()} to {end_date.isoformat()}"

        # Get overall KPIs from v_daily_kpis
        kpi_result = self._backend.query(f"""
            SELECT
                SUM(total_sessions) AS total_sessions,
                AVG(avg_urls_per_session) AS avg_urls,
                AVG(singleton_rate) AS singleton_pct,
                AVG(mean_mibcs_multi_url) AS mean_mibcs
            FROM v_daily_kpis
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
        """)

        kpis = kpi_result[0] if kpi_result else {}

        # Get URL distribution from v_session_url_distribution
        dist_result = self._backend.query(f"""
            SELECT
                url_bucket,
                SUM(session_count) AS sessions
            FROM v_session_url_distribution
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
            GROUP BY url_bucket
            ORDER BY sort_order
        """)

        url_distribution = {row["url_bucket"]: row["sessions"] for row in dist_result}

        # Get top bots from v_bot_volume
        bots_result = self._backend.query(f"""
            SELECT
                bot_name,
                SUM(session_count) AS sessions
            FROM v_bot_volume
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
            GROUP BY bot_name
            ORDER BY sessions DESC
            LIMIT 10
        """)

        # Get high confidence rate
        confidence_result = self._backend.query(f"""
            SELECT
                ROUND(100.0 * SUM(high_confidence_count) / SUM(total_sessions), 2) AS rate
            FROM v_daily_kpis
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
        """)

        high_conf_rate = (
            confidence_result[0]["rate"]
            if confidence_result and confidence_result[0]["rate"]
            else 0.0
        )

        return DashboardMetrics(
            total_sessions=kpis.get("total_sessions") or 0,
            avg_urls_per_session=kpis.get("avg_urls") or 0.0,
            singleton_rate=kpis.get("singleton_pct") or 0.0,
            mean_mibcs_multi_url=kpis.get("mean_mibcs"),
            high_confidence_rate=high_conf_rate,
            url_distribution=url_distribution,
            top_bots=bots_result,
            period=period,
        )

    def get_kpi_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> dict[str, Any]:
        """
        Get KPI summary for date range using v_daily_kpis view.

        Reference query implementation.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            Dictionary with aggregated KPIs
        """
        self.initialize()

        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        result = self._backend.query(f"""
            SELECT
                SUM(total_sessions) AS total_sessions,
                AVG(avg_urls_per_session) AS avg_urls,
                AVG(singleton_rate) AS singleton_pct,
                AVG(mean_mibcs_multi_url) AS mean_mibcs
            FROM v_daily_kpis
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
        """)

        return {
            "period": f"{start_date.isoformat()} to {end_date.isoformat()}",
            "metrics": result[0] if result else {},
        }

    def get_url_distribution(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> list[dict[str, Any]]:
        """
        Get URL distribution using v_session_url_distribution view.

        Reference query implementation.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            List of bucket distribution dictionaries
        """
        self.initialize()

        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        return self._backend.query(f"""
            SELECT
                url_bucket,
                SUM(session_count) AS sessions,
                ROUND(
                    100.0 * SUM(session_count) /
                    (SELECT SUM(session_count) FROM v_session_url_distribution
                     WHERE session_date >= '{start_date.isoformat()}'
                       AND session_date <= '{end_date.isoformat()}'),
                    1
                ) AS pct
            FROM v_session_url_distribution
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
            GROUP BY url_bucket
            ORDER BY sort_order
        """)

    def get_top_bots(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Get top bots by session count using v_bot_volume view.

        Reference query implementation.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            limit: Number of bots to return (default: 10)

        Returns:
            List of bot dictionaries with session counts
        """
        self.initialize()

        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        return self._backend.query(f"""
            SELECT
                bot_name,
                SUM(session_count) AS sessions
            FROM v_bot_volume
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
            GROUP BY bot_name
            ORDER BY sessions DESC
            LIMIT {limit}
        """)
