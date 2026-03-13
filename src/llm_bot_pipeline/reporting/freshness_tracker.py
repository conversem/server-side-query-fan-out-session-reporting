"""
Data freshness tracking for ETL pipeline monitoring.

Tracks when tables were last processed and records processing metadata.
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

from ..storage import StorageBackend, get_backend

logger = logging.getLogger(__name__)

# Table dependency graph: upstream -> downstream tables
# If an upstream table changes, downstream tables may need refresh
TABLE_DEPENDENCIES = {
    "bot_requests_daily": ["daily_summary", "url_performance", "query_fanout_sessions"],
    "daily_summary": [],
    "url_performance": [],
    "query_fanout_sessions": [],
}


@dataclass
class FreshnessRecord:
    """Record of data freshness for a table."""

    table_name: str
    last_processed_date: date
    last_updated_at: datetime
    rows_processed: int


class DataFreshnessTracker:
    """
    Tracks ETL run metadata in the data_freshness table.

    Works with both SQLite and BigQuery backends through StorageBackend.
    Uses upsert logic to update existing entries for incremental runs.

    Usage:
        tracker = DataFreshnessTracker(backend_type="sqlite", db_path=Path("data.db"))
        tracker.initialize()

        # After processing a table
        tracker.update_freshness(
            table_name="daily_summary",
            last_processed_date=date.today(),
            rows_processed=1234,
        )

        # Query when table was last processed
        last_date = tracker.get_last_processed_date("daily_summary")
    """

    TABLE_NAME = "data_freshness"

    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        backend_type: str = "sqlite",
        db_path: Optional[str] = None,
        **backend_kwargs,
    ):
        """
        Initialize data freshness tracker.

        Args:
            backend: Pre-initialized StorageBackend (optional)
            backend_type: Backend type if creating new ('sqlite' or 'bigquery')
            db_path: Path to SQLite database (for sqlite backend)
            **backend_kwargs: Additional kwargs for backend initialization
        """
        if backend:
            self._backend = backend
            self._owns_backend = False
        else:
            kwargs = {**backend_kwargs}
            if backend_type == "sqlite" and db_path:
                kwargs["db_path"] = db_path
            self._backend = get_backend(backend_type, **kwargs)
            self._owns_backend = True

        self._initialized = False

    def initialize(self) -> None:
        """Initialize the backend (create tables if needed)."""
        if not self._initialized:
            self._backend.initialize()
            self._initialized = True

    def close(self) -> None:
        """Close the backend connection."""
        if self._owns_backend:
            self._backend.close()

    def __enter__(self) -> "DataFreshnessTracker":
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def _get_table_ref(self) -> str:
        """Get the table reference for SQL queries."""
        if self._backend.backend_type == "bigquery":
            if hasattr(self._backend, "_get_full_table_id"):
                return f"`{self._backend._get_full_table_id(self.TABLE_NAME)}`"
        return self.TABLE_NAME

    def update_freshness(
        self,
        table_name: str,
        last_processed_date: date,
        rows_processed: int,
    ) -> bool:
        """
        Update or insert freshness record for a table.

        Uses upsert logic: updates existing entry or inserts new one.

        Args:
            table_name: Name of the table that was processed
            last_processed_date: Last date that was processed
            rows_processed: Number of rows processed/inserted

        Returns:
            True if update was successful
        """
        self.initialize()
        now = datetime.now(timezone.utc)

        try:
            # Check if entry exists
            existing = self.get_freshness_record(table_name)

            table_ref = self._get_table_ref()

            if existing:
                # Update existing entry
                sql = f"""
                    UPDATE {table_ref}
                    SET last_processed_date = :last_processed_date,
                        last_updated_at = :last_updated_at,
                        rows_processed = :rows_processed
                    WHERE table_name = :table_name
                """
            else:
                # Insert new entry
                sql = f"""
                    INSERT INTO {table_ref} (
                        table_name, last_processed_date, last_updated_at, rows_processed
                    ) VALUES (
                        :table_name, :last_processed_date, :last_updated_at, :rows_processed
                    )
                """

            params = {
                "table_name": table_name,
                "last_processed_date": last_processed_date.isoformat(),
                "last_updated_at": now.isoformat(),
                "rows_processed": rows_processed,
            }

            self._backend.execute(sql, params)
            logger.info(
                f"Updated freshness for {table_name}: "
                f"date={last_processed_date}, rows={rows_processed:,}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to update freshness for {table_name}: {e}")
            return False

    def get_freshness_record(self, table_name: str) -> Optional[FreshnessRecord]:
        """
        Get freshness record for a table.

        Args:
            table_name: Name of the table to query

        Returns:
            FreshnessRecord if exists, None otherwise
        """
        self.initialize()
        table_ref = self._get_table_ref()

        try:
            sql = f"""
                SELECT table_name, last_processed_date, last_updated_at, rows_processed
                FROM {table_ref}
                WHERE table_name = :table_name
            """
            results = self._backend.query(sql, {"table_name": table_name})

            if not results:
                return None

            row = results[0]
            return FreshnessRecord(
                table_name=row["table_name"],
                last_processed_date=self._parse_date(row["last_processed_date"]),
                last_updated_at=self._parse_datetime(row["last_updated_at"]),
                rows_processed=row["rows_processed"],
            )

        except Exception as e:
            logger.debug(f"Could not get freshness record for {table_name}: {e}")
            return None

    def get_last_processed_date(self, table_name: str) -> Optional[date]:
        """
        Get the last processed date for a table.

        Args:
            table_name: Name of the table to query

        Returns:
            Last processed date, or None if table has never been processed
        """
        record = self.get_freshness_record(table_name)
        return record.last_processed_date if record else None

    def get_all_freshness_records(self) -> list[FreshnessRecord]:
        """
        Get all freshness records.

        Returns:
            List of FreshnessRecord for all tracked tables
        """
        self.initialize()
        table_ref = self._get_table_ref()

        try:
            sql = f"""
                SELECT table_name, last_processed_date, last_updated_at, rows_processed
                FROM {table_ref}
                ORDER BY table_name
            """
            results = self._backend.query(sql)

            return [
                FreshnessRecord(
                    table_name=row["table_name"],
                    last_processed_date=self._parse_date(row["last_processed_date"]),
                    last_updated_at=self._parse_datetime(row["last_updated_at"]),
                    rows_processed=row["rows_processed"],
                )
                for row in results
            ]

        except Exception as e:
            logger.warning(f"Could not get all freshness records: {e}")
            return []

    def get_stale_tables(self, max_age_days: int = 1) -> list[str]:
        """
        Get list of tables that haven't been updated recently.

        Args:
            max_age_days: Tables not updated within this many days are considered stale

        Returns:
            List of stale table names
        """
        records = self.get_all_freshness_records()
        now = datetime.now(timezone.utc)
        stale_threshold = now.timestamp() - (max_age_days * 86400)

        stale = []
        for record in records:
            if record.last_updated_at.timestamp() < stale_threshold:
                stale.append(record.table_name)

        return stale

    def _parse_date(self, value) -> date:
        """Parse date from various formats."""
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        raise ValueError(f"Cannot parse date from {type(value)}: {value}")

    def _parse_datetime(self, value) -> datetime:
        """Parse datetime from various formats."""
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        raise ValueError(f"Cannot parse datetime from {type(value)}: {value}")

    def get_stale_downstream_tables(
        self, upstream_table: str = "bot_requests_daily"
    ) -> list[str]:
        """
        Get downstream tables that are stale relative to an upstream table.

        A downstream table is considered stale if its last_processed_date
        is older than the upstream table's last_processed_date.

        Args:
            upstream_table: The upstream table to compare against

        Returns:
            List of downstream table names that need refresh
        """
        upstream_record = self.get_freshness_record(upstream_table)
        if not upstream_record:
            logger.debug(f"No freshness record for upstream table: {upstream_table}")
            return []

        downstream_tables = TABLE_DEPENDENCIES.get(upstream_table, [])
        stale = []

        for downstream in downstream_tables:
            downstream_record = self.get_freshness_record(downstream)
            if not downstream_record:
                # No record means never processed - definitely stale
                stale.append(downstream)
            elif (
                downstream_record.last_processed_date
                < upstream_record.last_processed_date
            ):
                stale.append(downstream)

        return stale

    def get_dependency_status(self) -> dict[str, dict]:
        """
        Get dependency status for all tables.

        Returns:
            Dictionary with table status and dependency information
        """
        status = {}

        for table, downstream in TABLE_DEPENDENCIES.items():
            record = self.get_freshness_record(table)
            stale_downstream = (
                self.get_stale_downstream_tables(table) if downstream else []
            )

            status[table] = {
                "last_processed_date": record.last_processed_date if record else None,
                "last_updated_at": record.last_updated_at if record else None,
                "rows_processed": record.rows_processed if record else 0,
                "downstream_tables": downstream,
                "stale_downstream": stale_downstream,
            }

        return status
