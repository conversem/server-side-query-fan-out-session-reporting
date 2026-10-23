"""Tests for specific exception handling in pipeline modules."""

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from llm_bot_pipeline.pipeline.exceptions import PipelineError
from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline
from llm_bot_pipeline.storage import StorageError


class TestOrchestratorPropagatesAttributeError:
    """AttributeError (programming bug) must NOT be silently swallowed."""

    def test_orchestrator_propagates_attribute_error(self, tmp_path):
        pipeline = LocalPipeline(backend_type="sqlite", db_path=tmp_path / "test.db")
        pipeline.initialize()

        with patch.object(
            pipeline, "_execute_pipeline", side_effect=AttributeError("bad attr")
        ):
            with pytest.raises(AttributeError, match="bad attr"):
                pipeline.run(
                    start_date=date(2026, 1, 1),
                    end_date=date(2026, 1, 1),
                )

        pipeline.close()


class TestOrchestratorCatchesStorageError:
    """StorageError should be caught gracefully and reported in result."""

    def test_orchestrator_catches_storage_error(self, tmp_path):
        pipeline = LocalPipeline(backend_type="sqlite", db_path=tmp_path / "test.db")
        pipeline.initialize()

        with patch.object(
            pipeline,
            "_execute_pipeline",
            side_effect=StorageError("disk full"),
        ):
            result = pipeline.run(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 1),
            )

        assert not result.success
        assert any("disk full" in e for e in result.errors)
        pipeline.close()

    def test_orchestrator_catches_pipeline_error(self, tmp_path):
        pipeline = LocalPipeline(backend_type="sqlite", db_path=tmp_path / "test.db")
        pipeline.initialize()

        with patch.object(
            pipeline,
            "_execute_pipeline",
            side_effect=PipelineError("transform broken"),
        ):
            result = pipeline.run(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 1),
            )

        assert not result.success
        assert any("transform broken" in e for e in result.errors)
        pipeline.close()


class TestTopLevelPropagatesKeyboardInterrupt:
    """KeyboardInterrupt must propagate through run_daily_pipeline."""

    def test_keyboard_interrupt_not_caught(self):
        from llm_bot_pipeline.pipeline.daily.orchestrator import run_daily_pipeline

        with patch(
            "llm_bot_pipeline.pipeline.daily.orchestrator.load_pipeline_config",
            side_effect=KeyboardInterrupt,
        ):
            with pytest.raises(KeyboardInterrupt):
                run_daily_pipeline(
                    config_path=Path("config.enc.yaml"),
                    default_sops_key=Path("keys.txt"),
                    start_date=date(2026, 1, 1),
                    end_date=date(2026, 1, 1),
                )

    def test_system_exit_not_caught(self):
        from llm_bot_pipeline.pipeline.daily.orchestrator import run_daily_pipeline

        with patch(
            "llm_bot_pipeline.pipeline.daily.orchestrator.load_pipeline_config",
            side_effect=SystemExit(1),
        ):
            with pytest.raises(SystemExit):
                run_daily_pipeline(
                    config_path=Path("config.enc.yaml"),
                    default_sops_key=Path("keys.txt"),
                    start_date=date(2026, 1, 1),
                    end_date=date(2026, 1, 1),
                )
