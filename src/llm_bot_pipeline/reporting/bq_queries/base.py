"""
Base class for BigQuery dashboard queries.

Provides BigQuery client initialization and query execution.
"""

import logging
from pathlib import Path
from typing import Any, Optional

from google.cloud import bigquery

from ...config.constants import (
    DATASET_REPORT,
    TABLE_CLEAN_BOT_REQUESTS,
    TABLE_DAILY_SUMMARY,
    TABLE_URL_PERFORMANCE,
)

logger = logging.getLogger(__name__)


class DashboardQueriesBase:
    """Base class: BQ client setup and low-level query execution."""

    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[Path] = None,
    ):
        """
        Initialize dashboard queries.

        Args:
            project_id: GCP project ID
            credentials_path: Path to service account JSON key (uses ADC if None)
        """
        self.project_id = project_id

        if credentials_path:
            self.client = bigquery.Client.from_service_account_json(
                str(credentials_path),
                project=project_id,
            )
        else:
            self.client = bigquery.Client(project=project_id)

        self.clean_table = f"{project_id}.{DATASET_REPORT}.{TABLE_CLEAN_BOT_REQUESTS}"
        self.daily_summary_table = (
            f"{project_id}.{DATASET_REPORT}.{TABLE_DAILY_SUMMARY}"
        )
        self.url_performance_table = (
            f"{project_id}.{DATASET_REPORT}.{TABLE_URL_PERFORMANCE}"
        )

        logger.info(f"DashboardQueries initialized for project: {project_id}")

    def _execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        result = self.client.query(query).result()
        return [dict(row.items()) for row in result]

    @staticmethod
    def _domain_filter(domain: Optional[str] = None) -> str:
        """Return a SQL AND clause for domain filtering, or empty string."""
        if domain:
            safe = domain.replace("'", "''")
            return f"AND domain = '{safe}'"
        return ""
