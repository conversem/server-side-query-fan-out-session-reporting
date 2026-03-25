"""Unit tests for CheckpointManager and local_bq_buffered checkpointing."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from llm_bot_pipeline.config.constants import TABLE_CLEAN_BOT_REQUESTS
from llm_bot_pipeline.pipeline.checkpoint import CheckpointManager
from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline
from llm_bot_pipeline.storage import StorageBackend


class TestCheckpointManager:
    """Tests for CheckpointManager class."""

    def test_checkpoint_created_after_sqlite_transform(self, tmp_path):
        """Verify checkpoint file is written after successful transform and insert."""
        path = tmp_path / "checkpoint.json"
        mgr = CheckpointManager(path)
        mgr.record_completed(
            date(2025, 1, 15),
            table=TABLE_CLEAN_BOT_REQUESTS,
            row_count=100,
        )
        assert path.exists()
        data = path.read_text()
        assert "2025-01-15" in data
        assert str(100) in data
        assert TABLE_CLEAN_BOT_REQUESTS in data

    def test_resume_skips_completed_dates(self, tmp_path):
        """Verify already-completed dates are skipped on re-run."""
        path = tmp_path / "checkpoint.json"
        mgr = CheckpointManager(path)
        mgr.record_completed(date(2025, 1, 15), row_count=50)
        mgr.record_completed(date(2025, 1, 16), row_count=60)
        completed = mgr.get_completed_dates(
            date(2025, 1, 14), date(2025, 1, 17), table=TABLE_CLEAN_BOT_REQUESTS
        )
        assert date(2025, 1, 15) in completed
        assert date(2025, 1, 16) in completed
        assert date(2025, 1, 14) not in completed
        assert date(2025, 1, 17) not in completed

    def test_partial_failure_records_progress(self, tmp_path):
        """Verify partial progress is saved when some dates succeed."""
        path = tmp_path / "checkpoint.json"
        mgr = CheckpointManager(path)
        mgr.record_completed(date(2025, 1, 10), row_count=10)
        mgr.record_completed(date(2025, 1, 11), row_count=20)
        assert path.exists()
        mgr2 = CheckpointManager(path)
        completed = mgr2.get_completed_dates(
            date(2025, 1, 1), date(2025, 1, 31), table=TABLE_CLEAN_BOT_REQUESTS
        )
        assert completed == {date(2025, 1, 10), date(2025, 1, 11)}

    def test_replace_existing_entry(self, tmp_path):
        """Re-recording same date replaces previous entry."""
        path = tmp_path / "checkpoint.json"
        mgr = CheckpointManager(path)
        mgr.record_completed(date(2025, 1, 15), row_count=100)
        mgr.record_completed(date(2025, 1, 15), row_count=150)
        completed = mgr.get_completed_dates(
            date(2025, 1, 15), date(2025, 1, 15), table=TABLE_CLEAN_BOT_REQUESTS
        )
        assert completed == {date(2025, 1, 15)}
        mgr._load()
        assert len(mgr._entries) == 1
        assert mgr._entries[0]["row_count"] == 150


class TestLocalPipelineCheckpointing:
    """Tests for LocalPipeline checkpoint integration."""

    def test_checkpoint_path_enables_per_date_processing(self, tmp_path):
        """Pipeline with checkpoint_path uses per-date flow for BQ output."""
        transform_row = [
            {
                "request_timestamp": "2025-01-15 12:00:00",
                "request_date": "2025-01-15",
                "request_hour": 12,
                "day_of_week": "Wednesday",
                "request_uri": "/",
                "request_host": "example.com",
                "url_path": "/",
                "user_agent_raw": "GPTBot/1.0",
                "bot_name": "GPTBot",
                "bot_provider": "OpenAI",
                "bot_category": "training",
                "crawler_country": "US",
                "response_status": 200,
                "response_status_category": "success",
                "_processed_at": "2025-01-15 12:00:00",
            }
        ]
        raw = MagicMock(spec=StorageBackend)
        raw.backend_type = "sqlite"
        raw.query = MagicMock(
            side_effect=[
                [{"cnt": 1}],
                [{"total_rows": 1, "unique_user_agents": 1}],
                [{"cnt": 1}],
                transform_row,
            ]
        )
        raw.table_exists = MagicMock(return_value=True)
        raw.get_table_row_count = MagicMock(return_value=0)

        out = MagicMock(spec=StorageBackend)
        out.backend_type = "bigquery"
        out.table_exists = MagicMock(return_value=True)
        out.execute = MagicMock(return_value=0)
        out.insert_clean_records = MagicMock(return_value=1)
        out.get_completed_dates_in_range = MagicMock(return_value=set())

        pipeline = LocalPipeline(
            backend=raw,
            output_backend=out,
            checkpoint_path=tmp_path / "ck.json",
        )
        pipeline.initialize()

        result = pipeline.run(
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 15),
            mode="full",
            dry_run=False,
        )
        assert result.success
        assert result.transformed_rows == 1
        ck_path = tmp_path / "ck.json"
        assert ck_path.exists()
        assert "2025-01-15" in ck_path.read_text()

    def test_skips_dates_in_checkpoint(self, tmp_path):
        """Dates in checkpoint are skipped on re-run."""
        ck_path = tmp_path / "ck.json"
        mgr = CheckpointManager(ck_path)
        mgr.record_completed(date(2025, 1, 15), row_count=10)
        mgr.record_completed(date(2025, 1, 16), row_count=20)

        raw = MagicMock(spec=StorageBackend)
        raw.backend_type = "sqlite"
        raw.query = MagicMock(
            side_effect=[
                [{"cnt": 100}],
                [{"total_rows": 100, "unique_user_agents": 10}],
            ]
        )
        raw.table_exists = MagicMock(return_value=True)
        raw.get_table_row_count = MagicMock(return_value=0)

        out = MagicMock(spec=StorageBackend)
        out.backend_type = "bigquery"
        out.table_exists = MagicMock(return_value=True)
        out.execute = MagicMock(return_value=0)
        out.insert_clean_records = MagicMock(return_value=0)
        out.get_completed_dates_in_range = MagicMock(return_value=set())

        pipeline = LocalPipeline(
            backend=raw,
            output_backend=out,
            checkpoint_path=ck_path,
        )
        pipeline.initialize()

        result = pipeline.run(
            start_date=date(2025, 1, 15),
            end_date=date(2025, 1, 16),
            mode="full",
            dry_run=False,
        )
        assert result.success
        assert result.transformed_rows == 0
        out.insert_clean_records.assert_not_called()
