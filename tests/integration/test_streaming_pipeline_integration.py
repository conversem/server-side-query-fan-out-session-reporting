"""
Integration tests for local_bq_streaming mode (StreamingPipeline).

Tests the full pipeline flow: IngestionRecord iterator -> PythonTransformer -> StorageBackend,
using a mock BigQuery backend that records all inserts for verification.
"""

from datetime import date, datetime, timezone
from typing import Iterator, Optional

import pytest

from llm_bot_pipeline.ingestion.base import IngestionRecord
from llm_bot_pipeline.pipeline.streaming_pipeline import (
    StreamingPipeline,
    StreamingPipelineResult,
)
from llm_bot_pipeline.storage.base import BackendCapabilities, StorageBackend


class MockBigQueryBackend(StorageBackend):
    """In-memory backend that records all insert_clean_records calls."""

    def __init__(self):
        self.inserted_records: list[dict] = []
        self.flush_sizes: list[int] = []
        self._initialized = False

    @property
    def backend_type(self) -> str:
        return "mock_bigquery"

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(supports_streaming=True)

    def initialize(self) -> None:
        self._initialized = True

    def close(self) -> None:
        pass

    def insert_clean_records(self, records: list[dict]) -> int:
        self.flush_sizes.append(len(records))
        self.inserted_records.extend(records)
        return len(records)

    def insert_raw_records(self, records: list[dict]) -> int:
        return 0

    def query(self, sql: str, params: Optional[dict] = None) -> list[dict]:
        return []

    def execute(self, sql: str, params: Optional[dict] = None) -> int:
        return 0

    def table_exists(self, table_name: str) -> bool:
        return False

    def get_table_row_count(self, table_name: str) -> int:
        return 0

    def insert_sitemap_urls(self, entries: list[dict]) -> int:
        return 0


BOT_USER_AGENT = "Mozilla/5.0 (compatible; GPTBot/1.2; +https://openai.com/gptbot)"
TEST_DATE = date(2024, 1, 15)


def _make_record(
    index: int,
    ts: Optional[datetime] = None,
    client_ip: Optional[str] = None,
    path: Optional[str] = None,
    host: str = "example.com",
    user_agent: str = BOT_USER_AGENT,
) -> IngestionRecord:
    """Build a single IngestionRecord with unique defaults per index."""
    if ts is None:
        ts = datetime(2024, 1, 15, 10, 0, index % 60, tzinfo=timezone.utc)
    return IngestionRecord(
        timestamp=ts,
        client_ip=client_ip or f"10.0.0.{index % 256}",
        method="GET",
        host=host,
        path=path or f"/page/{index}",
        status_code=200,
        user_agent=user_agent,
        extra={"BotScore": 5, "VerifiedBot": 1, "ClientCountry": "US"},
    )


def _make_records(n: int) -> Iterator[IngestionRecord]:
    """Generate n unique IngestionRecords."""
    for i in range(n):
        yield _make_record(i)


class TestStreamingFullFlow:
    """test_streaming_full_flow: feed 100 records, assert all arrive in mocked BQ backend."""

    def test_all_records_arrive(self):
        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=50)

        result = pipeline.run(
            _make_records(100),
            start_date=TEST_DATE,
            end_date=TEST_DATE,
        )

        assert result.success is True
        assert result.records_in == 100
        assert result.records_transformed == 100
        assert len(backend.inserted_records) == 100
        assert result.batches_flushed == 2
        assert result.errors == []

    def test_backend_initialized(self):
        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=100)

        pipeline.run(_make_records(5), start_date=TEST_DATE, end_date=TEST_DATE)

        assert backend._initialized is True

    def test_result_has_timing(self):
        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=100)

        result = pipeline.run(
            _make_records(10), start_date=TEST_DATE, end_date=TEST_DATE
        )

        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.duration_seconds >= 0


class TestStreamingDedupIntegration:
    """test_streaming_dedup_integration: feed duplicates, assert only unique records."""

    def test_exact_duplicates_removed(self):
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        dup_record = _make_record(0, ts=ts, client_ip="10.0.0.1", path="/dup")

        def records_with_dups() -> Iterator[IngestionRecord]:
            for _ in range(5):
                yield IngestionRecord(
                    timestamp=dup_record.timestamp,
                    client_ip=dup_record.client_ip,
                    method=dup_record.method,
                    host=dup_record.host,
                    path=dup_record.path,
                    status_code=dup_record.status_code,
                    user_agent=dup_record.user_agent,
                    extra=dict(dup_record.extra),
                )

        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=100)

        result = pipeline.run(
            records_with_dups(), start_date=TEST_DATE, end_date=TEST_DATE
        )

        assert result.success is True
        assert result.records_in == 5
        assert result.duplicates == 4
        assert result.records_transformed == 1
        assert len(backend.inserted_records) == 1

    def test_mixed_unique_and_duplicate(self):
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        def mixed_records() -> Iterator[IngestionRecord]:
            for i in range(50):
                yield _make_record(i)
            for i in range(50):
                yield IngestionRecord(
                    timestamp=ts,
                    client_ip="10.0.0.1",
                    method="GET",
                    host="example.com",
                    path="/dup-page",
                    status_code=200,
                    user_agent=BOT_USER_AGENT,
                    extra={"BotScore": 5, "VerifiedBot": 1, "ClientCountry": "US"},
                )

        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=200)

        result = pipeline.run(mixed_records(), start_date=TEST_DATE, end_date=TEST_DATE)

        assert result.success is True
        assert result.records_in == 100
        assert result.duplicates == 49
        assert result.records_transformed == 51
        assert len(backend.inserted_records) == 51


class TestStreamingBatchBoundaries:
    """test_streaming_batch_boundaries: batch_size=10, 25 records -> 3 flushes."""

    def test_batch_flush_count(self):
        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=10)

        result = pipeline.run(
            _make_records(25), start_date=TEST_DATE, end_date=TEST_DATE
        )

        assert result.success is True
        assert result.records_in == 25
        assert result.batches_flushed == 3
        assert backend.flush_sizes == [10, 10, 5]
        assert len(backend.inserted_records) == 25

    def test_exact_multiple(self):
        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=10)

        result = pipeline.run(
            _make_records(30), start_date=TEST_DATE, end_date=TEST_DATE
        )

        assert result.batches_flushed == 3
        assert backend.flush_sizes == [10, 10, 10]
        assert len(backend.inserted_records) == 30

    def test_single_batch_partial(self):
        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=100)

        result = pipeline.run(
            _make_records(7), start_date=TEST_DATE, end_date=TEST_DATE
        )

        assert result.batches_flushed == 1
        assert backend.flush_sizes == [7]
        assert len(backend.inserted_records) == 7


class TestStreamingStatsAccurate:
    """test_streaming_stats_accurate: verify stats match actual processing."""

    def test_stats_match_records_processed(self):
        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=50)

        result = pipeline.run(
            _make_records(100), start_date=TEST_DATE, end_date=TEST_DATE
        )

        assert result.records_in == 100
        assert result.records_transformed == len(backend.inserted_records)
        assert result.records_filtered == 0
        assert result.duplicates == 0
        assert (
            result.records_in
            == result.records_transformed + result.records_filtered + result.duplicates
        )

    def test_stats_with_filtered_records(self):
        """Non-bot user agents are filtered out by PythonTransformer."""

        def mixed_ua_records() -> Iterator[IngestionRecord]:
            for i in range(50):
                yield _make_record(i, user_agent=BOT_USER_AGENT)
            for i in range(50, 100):
                yield _make_record(
                    i,
                    user_agent="Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36",
                )

        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=200)

        result = pipeline.run(
            mixed_ua_records(), start_date=TEST_DATE, end_date=TEST_DATE
        )

        assert result.success is True
        assert result.records_in == 100
        assert result.records_transformed == 50
        assert result.records_filtered == 50
        assert len(backend.inserted_records) == 50

    def test_to_dict_roundtrip(self):
        backend = MockBigQueryBackend()
        pipeline = StreamingPipeline(output_backend=backend, batch_size=50)

        result = pipeline.run(
            _make_records(20), start_date=TEST_DATE, end_date=TEST_DATE
        )

        d = result.to_dict()
        assert d["success"] is True
        assert d["records_in"] == 20
        assert d["records_transformed"] == 20
        assert d["duplicates"] == 0
        assert d["batches_flushed"] == 1
        assert d["duration_seconds"] is not None
        assert d["duration_seconds"] >= 0
