"""Unit tests for StreamingPipeline."""

import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

from llm_bot_pipeline.ingestion.base import IngestionRecord
from llm_bot_pipeline.pipeline.streaming_pipeline import StreamingPipeline
from llm_bot_pipeline.storage import StorageBackend, StorageError


def _make_record(
    user_agent: str, path: str = "/foo", host: str = "example.com"
) -> IngestionRecord:
    """Create an IngestionRecord with the given user_agent."""
    return IngestionRecord(
        timestamp=datetime(2025, 3, 4, 12, 0, 0, tzinfo=timezone.utc),
        client_ip="1.2.3.4",
        method="GET",
        host=host,
        path=path,
        status_code=200,
        user_agent=user_agent,
        extra={},
    )


class TestRunTransformsAndInserts:
    def test_run_transforms_and_inserts(self):
        """Known-bot records -> insert_clean_records called."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock()

        records = [
            _make_record(
                "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)",
                path="/a",
            ),
            _make_record(
                "Mozilla/5.0 (compatible; ClaudeBot/1.0; +https://anthropic.com)",
                path="/b",
            ),
        ]

        pipeline = StreamingPipeline(output_backend=backend, batch_size=10)
        result = pipeline.run(iter(records))

        assert result.success
        assert result.records_in == 2
        assert result.records_transformed == 2
        assert result.batches_flushed == 1
        backend.insert_clean_records.assert_called_once()
        call_args = backend.insert_clean_records.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0]["bot_name"] == "GPTBot"
        assert call_args[1]["bot_name"] == "ClaudeBot"


class TestRunFiltersUnknownBots:
    def test_run_filters_unknown_bots(self):
        """Non-bot records (Chrome UA) skipped."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock()

        records = [
            _make_record(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            ),
        ]

        pipeline = StreamingPipeline(output_backend=backend, batch_size=10)
        result = pipeline.run(iter(records))

        assert result.success
        assert result.records_in == 1
        assert result.records_transformed == 0
        assert result.records_filtered == 1
        assert result.batches_flushed == 0
        backend.insert_clean_records.assert_not_called()


class TestBatching:
    def test_batching(self):
        """batch_size=2, 5 records -> 2 full batches + 1 partial = 3 batches."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock()

        gptbot = "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)"
        records = [_make_record(gptbot, path=f"/p{i}") for i in range(5)]

        pipeline = StreamingPipeline(output_backend=backend, batch_size=2)
        result = pipeline.run(iter(records))

        assert result.success
        assert result.records_in == 5
        assert result.records_transformed == 5
        assert result.batches_flushed == 3
        assert backend.insert_clean_records.call_count == 3
        assert len(backend.insert_clean_records.call_args_list[0][0][0]) == 2
        assert len(backend.insert_clean_records.call_args_list[1][0][0]) == 2
        assert len(backend.insert_clean_records.call_args_list[2][0][0]) == 1


class TestEmptyIterator:
    def test_empty_iterator(self):
        """No records -> success with 0 stats."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock()

        pipeline = StreamingPipeline(output_backend=backend, batch_size=10)
        result = pipeline.run(iter([]))

        assert result.success
        assert result.records_in == 0
        assert result.records_transformed == 0
        assert result.records_filtered == 0
        assert result.duplicates == 0
        assert result.batches_flushed == 0
        assert result.errors == []
        assert result.completed_at is not None
        backend.insert_clean_records.assert_not_called()


class TestFlushFailurePropagates:
    def test_flush_failure_dead_letters(self):
        """StorageError -> retries exhausted -> dead-lettered, pipeline continues."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock(
            side_effect=StorageError("insert failed")
        )

        gptbot = "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)"
        records = [_make_record(gptbot)]

        from llm_bot_pipeline.monitoring.retry_handler import RetryConfig

        pipeline = StreamingPipeline(
            output_backend=backend,
            batch_size=10,
            retry_config=RetryConfig(max_retries=0, base_delay_seconds=0, jitter=False),
        )
        result = pipeline.run(iter(records))

        assert result.success  # pipeline continues after dead-lettering
        assert result.dead_lettered_count > 0
        assert len(result.errors) > 0
        assert any("batch_insert" in e or "insert failed" in e for e in result.errors)


class TestResultStats:
    def test_result_stats(self):
        """records_in, records_transformed, batches_flushed correct."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock()

        gptbot = "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)"
        records = [_make_record(gptbot, path=f"/path{i}") for i in range(3)]

        pipeline = StreamingPipeline(output_backend=backend, batch_size=2)
        result = pipeline.run(iter(records))

        assert result.records_in == 3
        assert result.records_transformed == 3
        assert result.batches_flushed == 2
        assert result.records_filtered == 0
        assert result.duplicates == 0


class TestBackpressure:
    def test_backpressure_blocks_when_limit_reached(self, caplog):
        """When max_pending_batches reached, backpressure logs warning and flushes."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock()

        gptbot = "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)"
        records = [_make_record(gptbot, path=f"/p{i}") for i in range(12)]

        pipeline = StreamingPipeline(
            output_backend=backend,
            batch_size=2,
            max_pending_batches=2,
        )
        with caplog.at_level(logging.WARNING):
            result = pipeline.run(iter(records))

        assert result.success
        assert result.records_in == 12
        assert result.batches_flushed == 6
        assert any(
            "Backpressure applied" in rec.message for rec in caplog.records
        ), "Expected backpressure warning when limit reached"

    def test_backpressure_unblocks_after_flush(self):
        """After blocking, flush completes and iteration resumes to end."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock()

        gptbot = "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)"
        records = [_make_record(gptbot, path=f"/p{i}") for i in range(20)]

        pipeline = StreamingPipeline(
            output_backend=backend,
            batch_size=2,
            max_pending_batches=2,
        )
        result = pipeline.run(iter(records))

        assert result.success
        assert result.records_in == 20
        assert result.records_transformed == 20
        assert result.batches_flushed == 10
        assert backend.insert_clean_records.call_count == 10
