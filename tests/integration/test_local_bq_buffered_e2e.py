"""E2E integration tests for local_bq_buffered mode.

Validates the full path: SQLite raw -> SQLite SQL transform -> BigQuery clean,
using a real SQLite backend for raw storage/transform and a mock BQ backend
for clean record output.
"""

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from llm_bot_pipeline.config.constants import (
    TABLE_CLEAN_BOT_REQUESTS,
    TABLE_RAW_BOT_REQUESTS,
)
from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline
from llm_bot_pipeline.storage import get_backend


class MockBigQueryBackend:
    """In-memory mock that mimics the BigQuery StorageBackend interface."""

    def __init__(self):
        self.backend_type = "bigquery"
        self._clean_records: list[dict] = []
        self._initialized = False

    def initialize(self):
        self._initialized = True

    def close(self):
        pass

    def table_exists(self, table_name: str) -> bool:
        if table_name == TABLE_CLEAN_BOT_REQUESTS:
            return len(self._clean_records) > 0
        return False

    def get_table_row_count(self, table_name: str) -> int:
        if table_name == TABLE_CLEAN_BOT_REQUESTS:
            return len(self._clean_records)
        return 0

    def insert_clean_records(self, records: list[dict]) -> int:
        self._clean_records.extend(records)
        return len(records)

    def execute(self, sql: str, params=None) -> int:
        return 0

    def query(self, sql: str, params=None) -> list[dict]:
        return []


def _make_seeded_sqlite(tmp_path: Path, num_records: int = 50):
    """Create a SQLite backend seeded with sample raw records."""
    from tests.conftest import generate_sample_records

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=2)

    db_path = tmp_path / "buffered_e2e.db"
    backend = get_backend("sqlite", db_path=db_path)
    backend.initialize()

    records = generate_sample_records(
        num_records=num_records,
        start_date=start_date,
        end_date=end_date,
    )
    backend.insert_raw_records(records)

    return backend, start_date, end_date


@pytest.fixture()
def buffered_env(tmp_path):
    """Real SQLite raw backend + mock BQ output backend + checkpoint path."""
    sqlite_backend, start_date, end_date = _make_seeded_sqlite(tmp_path)
    bq_mock = MockBigQueryBackend()
    checkpoint_path = tmp_path / "checkpoint.json"

    yield {
        "sqlite": sqlite_backend,
        "bq": bq_mock,
        "checkpoint_path": checkpoint_path,
        "start_date": start_date,
        "end_date": end_date,
    }
    sqlite_backend.close()


class TestBufferedRawToBQ:
    """Ingest raw records, run local transform, assert clean records reach mock BQ."""

    def test_buffered_raw_to_bq(self, buffered_env):
        env = buffered_env
        pipeline = LocalPipeline(
            backend=env["sqlite"],
            output_backend=env["bq"],
            checkpoint_path=env["checkpoint_path"],
        )

        result = pipeline.run(
            start_date=env["start_date"],
            end_date=env["end_date"],
            mode="full",
        )

        assert result.success
        assert result.raw_rows > 0
        assert result.transformed_rows > 0
        assert len(env["bq"]._clean_records) == result.transformed_rows

        for rec in env["bq"]._clean_records:
            assert rec["request_date"] is not None
            assert rec["bot_name"] is not None
            assert rec["bot_name"] != "Unknown"

    def test_transformed_records_have_required_fields(self, buffered_env):
        env = buffered_env
        pipeline = LocalPipeline(
            backend=env["sqlite"],
            output_backend=env["bq"],
            checkpoint_path=env["checkpoint_path"],
        )
        pipeline.run(
            start_date=env["start_date"],
            end_date=env["end_date"],
            mode="full",
        )

        expected_fields = {
            "request_timestamp",
            "request_date",
            "request_hour",
            "day_of_week",
            "request_uri",
            "request_host",
            "domain",
            "url_path",
            "url_path_depth",
            "user_agent_raw",
            "bot_name",
            "bot_provider",
            "bot_category",
            "bot_score",
            "is_verified_bot",
            "crawler_country",
            "response_status",
            "response_status_category",
            "_processed_at",
        }

        for rec in env["bq"]._clean_records:
            assert set(rec.keys()) == expected_fields

    def test_checkpoint_written_after_success(self, buffered_env):
        env = buffered_env
        pipeline = LocalPipeline(
            backend=env["sqlite"],
            output_backend=env["bq"],
            checkpoint_path=env["checkpoint_path"],
        )
        pipeline.run(
            start_date=env["start_date"],
            end_date=env["end_date"],
            mode="full",
        )

        assert env["checkpoint_path"].exists()

        import json

        data = json.loads(env["checkpoint_path"].read_text())
        completed_dates = {e["date"] for e in data["completed"]}
        assert len(completed_dates) > 0


class TestBufferedSqliteHasRawOnly:
    """After a run, SQLite keeps raw records; clean records go to BQ only."""

    def test_buffered_sqlite_has_raw_only(self, buffered_env):
        env = buffered_env
        pipeline = LocalPipeline(
            backend=env["sqlite"],
            output_backend=env["bq"],
            checkpoint_path=env["checkpoint_path"],
        )
        pipeline.run(
            start_date=env["start_date"],
            end_date=env["end_date"],
            mode="full",
        )

        raw_count = env["sqlite"].get_table_row_count(TABLE_RAW_BOT_REQUESTS)
        assert raw_count > 0

        clean_in_sqlite = env["sqlite"].query(
            f"SELECT COUNT(*) as cnt FROM {TABLE_CLEAN_BOT_REQUESTS}"
        )
        assert clean_in_sqlite[0]["cnt"] == 0

        assert len(env["bq"]._clean_records) > 0

    def test_raw_count_matches_original_ingestion(self, buffered_env):
        env = buffered_env
        raw_before = env["sqlite"].get_table_row_count(TABLE_RAW_BOT_REQUESTS)

        pipeline = LocalPipeline(
            backend=env["sqlite"],
            output_backend=env["bq"],
            checkpoint_path=env["checkpoint_path"],
        )
        pipeline.run(
            start_date=env["start_date"],
            end_date=env["end_date"],
            mode="full",
        )

        raw_after = env["sqlite"].get_table_row_count(TABLE_RAW_BOT_REQUESTS)
        assert raw_after == raw_before
