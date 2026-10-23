"""Shared data-quality runner for cloud jobs and CLIs."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

from ..config.settings import DomainConfig
from ..storage import StorageBackend, get_backend
from .checkers.models import DataQualityReport
from .data_quality import DataQualityChecker


def create_quality_backend(
    backend_type: str,
    project_id: Optional[str] = None,
    credentials_path: Optional[str | Path] = None,
    db_path: Optional[str | Path] = None,
) -> StorageBackend:
    """Create the backend used by quality checks."""
    kwargs = {}
    if backend_type == "bigquery":
        if project_id:
            kwargs["project_id"] = project_id
        if credentials_path:
            kwargs["credentials_path"] = credentials_path
    elif backend_type == "sqlite" and db_path:
        kwargs["db_path"] = db_path
    return get_backend(backend_type, **kwargs)


def run_quality_checks(
    backend: StorageBackend,
    domain_configs: list[DomainConfig],
    check_date: Optional[date] = None,
    table_name: str = "bot_requests_daily",
    skip_variance: bool = False,
) -> DataQualityReport:
    """Run unified checks against an initialized backend."""
    checker = DataQualityChecker(
        backend=backend,
        table_name=table_name,
        domain_configs=domain_configs,
    )
    return checker.run_all_checks(
        table_name=table_name,
        check_date=check_date,
        skip_variance=skip_variance,
    )


def run_quality_checks_with_backend(
    backend_type: str,
    domain_configs: list[DomainConfig],
    check_date: Optional[date] = None,
    table_name: str = "bot_requests_daily",
    project_id: Optional[str] = None,
    credentials_path: Optional[str | Path] = None,
    db_path: Optional[str | Path] = None,
    skip_variance: bool = False,
) -> DataQualityReport:
    """Create, initialize, run, and close the backend for quality checks."""
    backend = create_quality_backend(
        backend_type=backend_type,
        project_id=project_id,
        credentials_path=credentials_path,
        db_path=db_path,
    )
    backend.initialize()
    try:
        return run_quality_checks(
            backend=backend,
            domain_configs=domain_configs,
            check_date=check_date,
            table_name=table_name,
            skip_variance=skip_variance,
        )
    finally:
        backend.close()
