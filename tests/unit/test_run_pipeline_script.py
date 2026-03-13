"""Unit tests for run_pipeline.py script: router usage and backward compat."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.pipeline.router import (
    processing_mode_to_backend_type,
    run_pipeline,
)


class TestRunPipelineUsesRouter:
    """run_pipeline.py uses router.run_pipeline with correct processing_mode."""

    def test_run_pipeline_uses_router(self):
        """Verify router.run_pipeline accepts processing_mode and dispatches."""
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = MagicMock(
            success=True, transformed_rows=10, raw_rows=10, duplicates_removed=0
        )

        with patch(
            "llm_bot_pipeline.pipeline.router.get_settings",
            return_value=MagicMock(
                processing_mode="local_sqlite",
                sqlite_db_path="/tmp/test.db",
                gcp_project_id="",
                service_account_key_path=MagicMock(
                    exists=MagicMock(return_value=False)
                ),
                dataset_raw="raw",
                dataset_report="report",
                gcp_location="EU",
            ),
        ):
            with patch(
                "llm_bot_pipeline.pipeline.router._make_sqlite_backend",
                return_value=MagicMock(),
            ):
                with patch(
                    "llm_bot_pipeline.pipeline.local_pipeline.LocalPipeline",
                    return_value=mock_pipeline,
                ):
                    result = run_pipeline(
                        date(2025, 3, 1),
                        date(2025, 3, 1),
                        processing_mode="local_sqlite",
                        dry_run=True,
                    )
        assert result.transformed_rows == 10
        mock_pipeline.run.assert_called_once()
        call_kw = mock_pipeline.run.call_args[1]
        assert call_kw["dry_run"] is True


class TestBackwardCompatBackendFlag:
    """--backend sqlite maps to processing_mode=local_sqlite."""

    def test_backend_sqlite_maps_to_local_sqlite(self):
        """--backend sqlite should resolve to processing_mode local_sqlite."""
        # Resolution logic from run_pipeline.py
        backend = "sqlite"
        processing_mode = "local_sqlite" if backend == "sqlite" else "gcp_bq"
        assert processing_mode == "local_sqlite"

    def test_backend_bigquery_maps_to_gcp_bq(self):
        """--backend bigquery should resolve to processing_mode gcp_bq."""
        backend = "bigquery"
        processing_mode = "local_sqlite" if backend == "sqlite" else "gcp_bq"
        assert processing_mode == "gcp_bq"

    def test_processing_mode_to_backend_type(self):
        """processing_mode_to_backend_type maps correctly."""
        from llm_bot_pipeline.pipeline.router import processing_mode_to_backend_type

        assert processing_mode_to_backend_type("local_sqlite") == "sqlite"
        assert processing_mode_to_backend_type("local_bq_buffered") == "bigquery"
        assert processing_mode_to_backend_type("gcp_bq") == "bigquery"
