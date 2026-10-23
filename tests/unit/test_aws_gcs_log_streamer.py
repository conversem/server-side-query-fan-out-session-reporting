"""Unit tests for gcs_log_streamer: list + stream GCS blobs as text IO.

Tests use a fake google.cloud.storage client; no real GCS calls.
"""

import gzip
import io
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.aws.gcs_log_streamer import (
    _blob_partition_date,
    stream_log_handles,
)


class _FakeBlob:
    def __init__(self, name: str, data: bytes):
        self.name = name
        self._data = data
        self.handles: list[io.BytesIO] = []
        self.fetched = False

    def open(self, mode: str = "rb"):
        self.fetched = True
        handle = io.BytesIO(self._data)
        self.handles.append(handle)
        return handle

    def download_as_bytes(self):
        self.fetched = True
        return self._data


def _fake_client_with_blobs(blobs: list[_FakeBlob]):
    client = MagicMock()
    bucket = MagicMock()
    client.bucket.return_value = bucket
    bucket.list_blobs.return_value = iter(blobs)
    return client


def test_yields_blob_name_and_decoded_handle():
    payload = b"line one\nline two\n"
    with patch("llm_bot_pipeline.aws.gcs_log_streamer.storage") as mock_storage:
        mock_storage.Client.return_value = _fake_client_with_blobs(
            [_FakeBlob("logs/file.txt", payload)]
        )
        results = list(stream_log_handles("gs://bucket/logs/"))
    assert len(results) == 1
    name, handle = results[0]
    assert name == "logs/file.txt"
    assert handle.read() == "line one\nline two\n"


def test_decompresses_gzip_transparently():
    raw = b"col1 col2\nvalue1 value2\n"
    gz = gzip.compress(raw)
    blob = _FakeBlob("logs/file.gz", gz)
    with patch("llm_bot_pipeline.aws.gcs_log_streamer.storage") as mock_storage:
        mock_storage.Client.return_value = _fake_client_with_blobs([blob])
        results = list(stream_log_handles("gs://bucket/logs/"))
    name, handle = results[0]
    assert name == "logs/file.gz"
    assert handle.read() == raw.decode("utf-8")


def test_sequential_mode_closes_underlying_stream():
    """max_workers=1 streams via blob.open; closing the text handle closes it."""
    raw = b"col1 col2\nvalue1 value2\n"
    blob = _FakeBlob("logs/file.gz", gzip.compress(raw))
    with patch("llm_bot_pipeline.aws.gcs_log_streamer.storage") as mock_storage:
        mock_storage.Client.return_value = _fake_client_with_blobs([blob])
        results = list(stream_log_handles("gs://bucket/logs/", max_workers=1))
    _, handle = results[0]
    assert handle.read() == raw.decode("utf-8")
    handle.close()
    assert blob.handles[0].closed


def test_parallel_prefetch_preserves_order_and_downloads_via_bytes():
    """Concurrent path yields in listing order and uses download_as_bytes."""
    blobs = [_FakeBlob(f"logs/f{i}.txt", f"row{i}\n".encode()) for i in range(40)]
    with patch("llm_bot_pipeline.aws.gcs_log_streamer.storage") as mock_storage:
        mock_storage.Client.return_value = _fake_client_with_blobs(blobs)
        results = list(stream_log_handles("gs://bucket/logs/", max_workers=8))
    assert [n for n, _ in results] == [f"logs/f{i}.txt" for i in range(40)]
    assert [h.read() for _, h in results] == [f"row{i}\n" for i in range(40)]
    assert all(b.fetched and not b.handles for b in blobs)


def test_skips_directory_placeholders():
    """GCS often has zero-byte 'directory' blobs ending in '/'."""
    with patch("llm_bot_pipeline.aws.gcs_log_streamer.storage") as mock_storage:
        mock_storage.Client.return_value = _fake_client_with_blobs(
            [
                _FakeBlob("logs/", b""),
                _FakeBlob("logs/file.txt", b"data\n"),
            ]
        )
        results = list(stream_log_handles("gs://bucket/logs/"))
    assert [n for n, _ in results] == ["logs/file.txt"]


def test_rejects_non_gs_prefix():
    with pytest.raises(ValueError, match="gs://"):
        list(stream_log_handles("s3://wrong-scheme/path/"))


ALB_BASE = "alb/mc/AWSLogs/594802546463/elasticloadbalancing/eu-central-1"


class TestBlobPartitionDate:
    def test_parses_alb_date_path(self):
        assert _blob_partition_date(
            f"{ALB_BASE}/2026/07/02/x_elasticloadbalancing_..._end.log.gz"
        ) == date(2026, 7, 2)

    def test_returns_none_without_date_path(self):
        assert _blob_partition_date("cloudfront/EXAMPLE.2026-07-02-13.abc.gz") is None

    def test_returns_none_for_invalid_date(self):
        assert _blob_partition_date(f"{ALB_BASE}/2026/13/40/bad.log.gz") is None


class TestDateRangeFiltering:
    def _blobs_across_days(self):
        return [
            _FakeBlob(f"{ALB_BASE}/2026/06/30/dev.log.gz", gzip.compress(b"jun30\n")),
            _FakeBlob(f"{ALB_BASE}/2026/07/01/a.log.gz", gzip.compress(b"jul01\n")),
            _FakeBlob(f"{ALB_BASE}/2026/07/02/b.log.gz", gzip.compress(b"jul02\n")),
            _FakeBlob(f"{ALB_BASE}/2026/07/05/c.log.gz", gzip.compress(b"jul05\n")),
        ]

    def test_only_in_range_blobs_downloaded(self):
        blobs = self._blobs_across_days()
        with patch("llm_bot_pipeline.aws.gcs_log_streamer.storage") as mock_storage:
            mock_storage.Client.return_value = _fake_client_with_blobs(blobs)
            results = list(
                stream_log_handles(
                    "gs://bucket/alb/mc/",
                    date_range=(date(2026, 7, 1), date(2026, 7, 2)),
                )
            )
        names = [n for n, _ in results]
        assert names == [
            f"{ALB_BASE}/2026/07/01/a.log.gz",
            f"{ALB_BASE}/2026/07/02/b.log.gz",
        ]
        # Out-of-range objects must never be fetched (no download cost).
        assert not blobs[0].fetched  # Jun 30
        assert not blobs[3].fetched  # Jul 05
        assert blobs[1].fetched and blobs[2].fetched

    def test_no_date_range_reads_everything(self):
        blobs = self._blobs_across_days()
        with patch("llm_bot_pipeline.aws.gcs_log_streamer.storage") as mock_storage:
            mock_storage.Client.return_value = _fake_client_with_blobs(blobs)
            results = list(stream_log_handles("gs://bucket/alb/mc/"))
        assert len(results) == 4

    def test_undated_blobs_always_included(self):
        blobs = [_FakeBlob("misc/no-date.log.gz", gzip.compress(b"x\n"))]
        with patch("llm_bot_pipeline.aws.gcs_log_streamer.storage") as mock_storage:
            mock_storage.Client.return_value = _fake_client_with_blobs(blobs)
            results = list(
                stream_log_handles(
                    "gs://bucket/misc/",
                    date_range=(date(2026, 7, 1), date(2026, 7, 2)),
                )
            )
        assert len(results) == 1
