"""Unit tests for StreamingPipeline retry logic and dead-letter support."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from llm_bot_pipeline.ingestion.base import IngestionRecord
from llm_bot_pipeline.monitoring.retry_handler import RetryConfig
from llm_bot_pipeline.pipeline.streaming_pipeline import StreamingPipeline
from llm_bot_pipeline.storage import StorageBackend, StorageError

GPTBOT_UA = "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)"


def _make_record(path: str = "/foo") -> IngestionRecord:
    return IngestionRecord(
        timestamp=datetime(2025, 3, 4, 12, 0, 0, tzinfo=timezone.utc),
        client_ip="1.2.3.4",
        method="GET",
        host="example.com",
        path=path,
        status_code=200,
        user_agent=GPTBOT_UA,
        extra={},
    )


def _no_sleep_retry_config(max_retries: int = 2) -> RetryConfig:
    """RetryConfig with zero delays for fast tests."""
    return RetryConfig(
        max_retries=max_retries,
        base_delay_seconds=0.0,
        max_delay_seconds=0.0,
        jitter=False,
    )


class TestFlushRetriesAndSucceeds:
    def test_flush_retries_and_succeeds(self):
        """Backend fails once then succeeds — batch is flushed normally."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock(
            side_effect=[StorageError("transient timeout"), None]
        )

        pipeline = StreamingPipeline(
            output_backend=backend,
            batch_size=10,
            retry_config=_no_sleep_retry_config(),
        )
        result = pipeline.run(iter([_make_record()]))

        assert result.success
        assert result.batches_flushed == 1
        assert result.dead_lettered_count == 0
        assert backend.insert_clean_records.call_count == 2


class TestFlushExhaustionDeadLetters:
    def test_flush_exhaustion_dead_letters(self, tmp_path):
        """All retries fail — batch is written to dead-letter file."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock(
            side_effect=StorageError("timeout unavailable")
        )

        dl_path = tmp_path / "dead_letter.jsonl"
        pipeline = StreamingPipeline(
            output_backend=backend,
            batch_size=10,
            retry_config=_no_sleep_retry_config(max_retries=1),
            dead_letter_path=str(dl_path),
        )
        result = pipeline.run(iter([_make_record(), _make_record("/bar")]))

        assert result.success  # pipeline continues
        assert result.batches_flushed == 0
        assert result.dead_lettered_count == 2
        assert dl_path.exists()
        assert len(result.errors) == 1


class TestDeadLetterFileFormat:
    def test_dead_letter_file_format(self, tmp_path):
        """Dead-letter file contains one valid JSON object per line."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock(
            side_effect=StorageError("timeout unavailable")
        )

        dl_path = tmp_path / "dl.jsonl"
        pipeline = StreamingPipeline(
            output_backend=backend,
            batch_size=10,
            retry_config=_no_sleep_retry_config(max_retries=0),
            dead_letter_path=str(dl_path),
        )
        records = [_make_record(f"/p{i}") for i in range(3)]
        pipeline.run(iter(records))

        lines = dl_path.read_text().strip().splitlines()
        assert len(lines) == 3
        for line in lines:
            obj = json.loads(line)
            assert "bot_name" in obj
            assert obj["bot_name"] == "GPTBot"


class TestPipelineContinuesAfterDeadLetter:
    def test_pipeline_continues_after_dead_letter(self, tmp_path):
        """After dead-lettering batch 1, batch 2 still flushes normally."""
        call_count = 0

        def _fail_first_succeed_rest(batch):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:  # first 2 calls (initial + 1 retry) fail
                raise StorageError("timeout unavailable")

        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock(side_effect=_fail_first_succeed_rest)

        dl_path = tmp_path / "dead_letter.jsonl"
        pipeline = StreamingPipeline(
            output_backend=backend,
            batch_size=2,
            retry_config=_no_sleep_retry_config(max_retries=1),
            dead_letter_path=str(dl_path),
        )
        records = [_make_record(f"/p{i}") for i in range(4)]
        result = pipeline.run(iter(records))

        assert result.success
        assert result.dead_lettered_count == 2  # first batch dead-lettered
        assert result.batches_flushed == 1  # second batch succeeded
        assert dl_path.exists()
