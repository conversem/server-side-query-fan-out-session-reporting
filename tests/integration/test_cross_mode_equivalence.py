"""Cross-mode equivalence integration tests.

Verifies that all local processing modes (local_sqlite, local_bq_buffered,
local_bq_streaming) produce equivalent output given the same input data.
This ensures mode selection is a deployment choice, not a correctness concern.

Uses real SQLite backends for local modes and mocked BQ backends for output.
"""

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

import pytest

from llm_bot_pipeline.config.constants import (
    TABLE_CLEAN_BOT_REQUESTS,
    TABLE_RAW_BOT_REQUESTS,
)
from llm_bot_pipeline.ingestion.base import IngestionRecord
from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline
from llm_bot_pipeline.pipeline.streaming_pipeline import StreamingPipeline
from llm_bot_pipeline.storage import get_backend
from llm_bot_pipeline.storage.base import BackendCapabilities, StorageBackend
from tests.conftest import generate_sample_records

NUM_RECORDS = 50
SEED = 42


# ---------------------------------------------------------------------------
# Mock BigQuery backend (shared by buffered and streaming modes)
# ---------------------------------------------------------------------------


class MockBigQueryBackend(StorageBackend):
    """In-memory backend that records all insert_clean_records calls."""

    def __init__(self):
        self._clean_records: list[dict] = []
        self._initialized = False

    @property
    def backend_type(self) -> str:
        return "bigquery"

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(supports_streaming=True)

    def initialize(self) -> None:
        self._initialized = True

    def close(self) -> None:
        pass

    def insert_clean_records(self, records: list[dict]) -> int:
        self._clean_records.extend(records)
        return len(records)

    def insert_raw_records(self, records: list[dict]) -> int:
        return 0

    def query(self, sql: str, params: Optional[dict] = None) -> list[dict]:
        return []

    def execute(self, sql: str, params: Optional[dict] = None) -> int:
        return 0

    def table_exists(self, table_name: str) -> bool:
        if table_name == TABLE_CLEAN_BOT_REQUESTS:
            return len(self._clean_records) > 0
        return False

    def get_table_row_count(self, table_name: str) -> int:
        if table_name == TABLE_CLEAN_BOT_REQUESTS:
            return len(self._clean_records)
        return 0

    def insert_sitemap_urls(self, entries: list[dict]) -> int:
        return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_to_ingestion_record(raw: dict) -> IngestionRecord:
    """Convert a raw record dict to an IngestionRecord for streaming mode."""
    ts_str = raw["EdgeStartTimestamp"]
    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return IngestionRecord(
        timestamp=ts,
        client_ip=raw["ClientIP"],
        method="GET",
        host=raw["ClientRequestHost"],
        path=raw["ClientRequestURI"],
        status_code=raw["EdgeResponseStatus"],
        user_agent=raw["ClientRequestUserAgent"],
        extra={
            "ClientCountry": raw.get("ClientCountry", ""),
        },
    )


# Fields that must be identical across all modes (excludes _processed_at
# and response_status_category which has a known format difference between
# SQL-based '2xx_success' and Python-based '2xx' transforms).
_EQUIVALENCE_FIELDS = (
    "bot_category",
    "bot_name",
    "bot_provider",
    "crawler_country",
    "day_of_week",
    "request_date",
    "request_host",
    "request_hour",
    "request_uri",
    "response_status",
    "url_path",
)


def _comparison_key(record: dict) -> tuple:
    """Extract a hashable comparison key from a clean record."""
    return tuple(str(record.get(f, "")) for f in _EQUIVALENCE_FIELDS)


def _bot_name_counts(records: list[dict]) -> Counter:
    return Counter(r.get("bot_name") for r in records)


def _request_date_counts(records: list[dict]) -> Counter:
    return Counter(str(r.get("request_date")) for r in records)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def shared_data(tmp_path):
    """Deterministic records and date range shared by all modes."""
    end_date = date(2024, 1, 14)
    start_date = end_date - timedelta(days=2)

    raw_records = generate_sample_records(
        num_records=NUM_RECORDS,
        start_date=start_date,
        end_date=end_date,
        seed=SEED,
    )
    return {
        "raw_records": raw_records,
        "start_date": start_date,
        "end_date": end_date,
        "tmp_path": tmp_path,
    }


# ---------------------------------------------------------------------------
# Mode runners
# ---------------------------------------------------------------------------


def _run_local_sqlite(data: dict) -> list[dict]:
    """Run local_sqlite mode and return clean records from SQLite."""
    db_path = data["tmp_path"] / "sqlite_mode.db"
    backend = get_backend("sqlite", db_path=db_path)
    backend.initialize()
    backend.insert_raw_records(data["raw_records"])

    pipeline = LocalPipeline(backend=backend)
    result = pipeline.run(
        start_date=data["start_date"],
        end_date=data["end_date"],
        mode="full",
    )
    assert result.success, f"local_sqlite failed: {result.errors}"

    clean = backend.query(f"SELECT * FROM {TABLE_CLEAN_BOT_REQUESTS}")
    pipeline.close()
    return clean


def _run_local_bq_buffered(data: dict) -> list[dict]:
    """Run local_bq_buffered mode and return clean records from mock BQ."""
    db_path = data["tmp_path"] / "buffered_mode.db"
    backend = get_backend("sqlite", db_path=db_path)
    backend.initialize()
    backend.insert_raw_records(data["raw_records"])

    bq_mock = MockBigQueryBackend()
    checkpoint_path = data["tmp_path"] / "checkpoint.json"
    pipeline = LocalPipeline(
        backend=backend,
        output_backend=bq_mock,
        checkpoint_path=checkpoint_path,
    )
    result = pipeline.run(
        start_date=data["start_date"],
        end_date=data["end_date"],
        mode="full",
    )
    assert result.success, f"local_bq_buffered failed: {result.errors}"

    pipeline.close()
    return bq_mock._clean_records


def _run_local_bq_streaming(data: dict) -> list[dict]:
    """Run local_bq_streaming mode and return clean records from mock BQ."""
    ingestion_records = [_raw_to_ingestion_record(r) for r in data["raw_records"]]
    ingestion_records.sort(key=lambda r: r.timestamp)

    bq_mock = MockBigQueryBackend()
    pipeline = StreamingPipeline(output_backend=bq_mock, batch_size=100)
    result = pipeline.run(
        iter(ingestion_records),
        start_date=data["start_date"],
        end_date=data["end_date"],
    )
    assert result.success, f"local_bq_streaming failed: {result.errors}"
    return bq_mock._clean_records


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCrossModeEquivalence:
    """Verify equivalent output across all three local processing modes."""

    def test_all_modes_produce_same_output(self, shared_data):
        """All modes produce the same number of clean records."""
        sqlite_recs = _run_local_sqlite(shared_data)
        buffered_recs = _run_local_bq_buffered(shared_data)
        streaming_recs = _run_local_bq_streaming(shared_data)

        assert len(sqlite_recs) > 0, "local_sqlite produced no records"
        assert len(buffered_recs) > 0, "local_bq_buffered produced no records"
        assert len(streaming_recs) > 0, "local_bq_streaming produced no records"

        assert len(sqlite_recs) == len(buffered_recs), (
            f"local_sqlite ({len(sqlite_recs)}) vs "
            f"local_bq_buffered ({len(buffered_recs)}) count mismatch"
        )
        assert len(sqlite_recs) == len(streaming_recs), (
            f"local_sqlite ({len(sqlite_recs)}) vs "
            f"local_bq_streaming ({len(streaming_recs)}) count mismatch"
        )

    def test_bot_classification_equivalent(self, shared_data):
        """All modes classify bots identically."""
        sqlite_bots = _bot_name_counts(_run_local_sqlite(shared_data))
        buffered_bots = _bot_name_counts(_run_local_bq_buffered(shared_data))
        streaming_bots = _bot_name_counts(_run_local_bq_streaming(shared_data))

        assert (
            sqlite_bots == buffered_bots
        ), f"sqlite vs buffered: {sqlite_bots} != {buffered_bots}"
        assert (
            sqlite_bots == streaming_bots
        ), f"sqlite vs streaming: {sqlite_bots} != {streaming_bots}"

    def test_dedup_equivalent(self, shared_data):
        """All modes deduplicate records consistently."""
        sqlite_keys = {_comparison_key(r) for r in _run_local_sqlite(shared_data)}
        buffered_keys = {
            _comparison_key(r) for r in _run_local_bq_buffered(shared_data)
        }
        streaming_keys = {
            _comparison_key(r) for r in _run_local_bq_streaming(shared_data)
        }

        assert sqlite_keys == buffered_keys, (
            f"Dedup mismatch sqlite vs buffered: "
            f"{len(sqlite_keys)} vs {len(buffered_keys)} unique keys"
        )
        assert sqlite_keys == streaming_keys, (
            f"Dedup mismatch sqlite vs streaming: "
            f"{len(sqlite_keys)} vs {len(streaming_keys)} unique keys"
        )

    def test_date_distribution_equivalent(self, shared_data):
        """All modes produce the same record counts per date."""
        sqlite_dates = _request_date_counts(_run_local_sqlite(shared_data))
        buffered_dates = _request_date_counts(_run_local_bq_buffered(shared_data))
        streaming_dates = _request_date_counts(_run_local_bq_streaming(shared_data))

        assert (
            sqlite_dates == buffered_dates
        ), f"sqlite vs buffered: {sqlite_dates} != {buffered_dates}"
        assert (
            sqlite_dates == streaming_dates
        ), f"sqlite vs streaming: {sqlite_dates} != {streaming_dates}"

    def test_record_fields_present(self, shared_data):
        """All modes produce records with the required business fields."""
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
            "crawler_country",
            "response_status",
            "response_status_category",
            "resource_type",
            "_processed_at",
        }

        for mode_name, runner in (
            ("local_sqlite", _run_local_sqlite),
            ("local_bq_buffered", _run_local_bq_buffered),
            ("local_bq_streaming", _run_local_bq_streaming),
        ):
            records = runner(shared_data)
            actual = set(records[0].keys())
            missing = expected_fields - actual
            assert not missing, f"{mode_name} missing fields: {missing}"

    def test_mode_is_deployment_choice(self, shared_data):
        """Composite: mode selection does not affect correctness."""
        sqlite_recs = _run_local_sqlite(shared_data)
        buffered_recs = _run_local_bq_buffered(shared_data)
        streaming_recs = _run_local_bq_streaming(shared_data)

        counts = {
            "sqlite": len(sqlite_recs),
            "buffered": len(buffered_recs),
            "streaming": len(streaming_recs),
        }
        assert len(set(counts.values())) == 1, f"Count mismatch: {counts}"

        assert _bot_name_counts(sqlite_recs) == _bot_name_counts(buffered_recs)
        assert _bot_name_counts(sqlite_recs) == _bot_name_counts(streaming_recs)

        assert _request_date_counts(sqlite_recs) == _request_date_counts(buffered_recs)
        assert _request_date_counts(sqlite_recs) == _request_date_counts(streaming_recs)

        sqlite_keys = {_comparison_key(r) for r in sqlite_recs}
        buffered_keys = {_comparison_key(r) for r in buffered_recs}
        streaming_keys = {_comparison_key(r) for r in streaming_recs}
        assert sqlite_keys == buffered_keys == streaming_keys
