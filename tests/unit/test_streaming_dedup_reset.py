"""Unit tests for dedup set reset in streaming mode."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from llm_bot_pipeline.ingestion.base import IngestionRecord
from llm_bot_pipeline.pipeline.python_transformer import PythonTransformer
from llm_bot_pipeline.pipeline.streaming_pipeline import StreamingPipeline
from llm_bot_pipeline.storage import StorageBackend


def _make_record(
    *,
    timestamp=None,
    path="/foo",
    host="example.com",
    user_agent="Mozilla/5.0 (compatible; GPTBot/1.0)",
):
    if timestamp is None:
        timestamp = datetime(2025, 3, 3, 12, 0, 0, tzinfo=timezone.utc)
    return IngestionRecord(
        timestamp=timestamp,
        client_ip="1.2.3.4",
        method="GET",
        host=host,
        path=path,
        status_code=200,
        user_agent=user_agent,
        extra={},
    )


class TestResetSeenClearsSet:
    """reset_seen() clears _seen set."""

    def test_reset_seen_clears_set(self):
        transformer = PythonTransformer()
        record = _make_record()

        transformer.transform(record)
        assert len(transformer._seen) == 1

        transformer.reset_seen()
        assert len(transformer._seen) == 0


class TestStreamingResetsBetweenDates:
    """Dedup only applies within same date, not across dates."""

    def test_streaming_resets_between_dates(self):
        """reset_seen() called when crossing date boundary."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock()

        records = [
            _make_record(
                timestamp=datetime(2025, 3, 3, 12, 0, 0, tzinfo=timezone.utc),
                path="/a",
            ),
            _make_record(
                timestamp=datetime(2025, 3, 4, 12, 0, 0, tzinfo=timezone.utc),
                path="/b",
            ),
        ]

        pipeline = StreamingPipeline(output_backend=backend, batch_size=10)
        with patch.object(pipeline._transformer, "reset_seen") as mock_reset:
            result = pipeline.run(iter(records))

        assert result.success
        assert result.records_in == 2
        assert result.records_transformed == 2
        mock_reset.assert_called_once()


class TestStreamingDedupWithinSameDate:
    """Within same date, duplicates are filtered."""

    def test_within_same_date_duplicates_filtered(self):
        """Same record twice on same date -> second is duplicate."""
        backend = MagicMock(spec=StorageBackend)
        backend.insert_clean_records = MagicMock()

        ts = datetime(2025, 3, 3, 12, 0, 0, tzinfo=timezone.utc)
        records = [
            _make_record(timestamp=ts, path="/same"),
            _make_record(timestamp=ts, path="/same"),
        ]

        pipeline = StreamingPipeline(output_backend=backend, batch_size=10)
        result = pipeline.run(iter(records))

        assert result.success
        assert result.records_in == 2
        assert result.records_transformed == 1
        assert result.duplicates == 1
