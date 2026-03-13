"""Unit tests for get_pipeline() factory function."""

from unittest.mock import patch

import pytest

from llm_bot_pipeline.pipeline import get_pipeline
from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline


class TestGetPipelineSQLite:
    def test_string_sqlite(self, tmp_path):
        pipeline = get_pipeline("sqlite", db_path=tmp_path / "test.db")
        assert isinstance(pipeline, LocalPipeline)

    def test_backend_instance_sqlite(self, tmp_path):
        from llm_bot_pipeline.storage import get_backend

        backend = get_backend("sqlite", db_path=tmp_path / "test.db")
        backend.initialize()
        pipeline = get_pipeline(backend)
        assert isinstance(pipeline, LocalPipeline)
        backend.close()


@pytest.mark.bigquery
class TestGetPipelineBigQuery:
    def test_string_bigquery(self):
        from llm_bot_pipeline.pipeline.orchestrator import ETLPipeline

        with (
            patch("llm_bot_pipeline.pipeline.orchestrator.LLMBotExtractor"),
            patch("llm_bot_pipeline.pipeline.orchestrator.LLMBotTransformer"),
        ):
            pipeline = get_pipeline("bigquery", project_id="test-project")
        assert isinstance(pipeline, ETLPipeline)


class TestGetPipelineErrors:
    def test_invalid_backend_type(self):
        with pytest.raises(ValueError, match="No pipeline for backend type"):
            get_pipeline("postgres")

    def test_invalid_argument_type(self):
        with pytest.raises(TypeError, match="Expected StorageBackend or str"):
            get_pipeline(12345)
