"""List and stream GCS blobs as text IO with transparent gzip."""

from __future__ import annotations

import gzip
import io
import re
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import IO, Iterator, Optional, Tuple
from urllib.parse import urlparse

try:
    from google.cloud import storage
except ImportError:
    storage = None

# ALB (and CloudFront-in-S3) access logs are delivered under a date-partitioned
# path segment: .../elasticloadbalancing/<region>/YYYY/MM/DD/<file>. We use this
# to skip downloading objects outside the requested window — the expensive part
# is the per-object download/decompress, not the (cheap) bucket listing.
_DATE_PATH_RE = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")


def _blob_partition_date(blob_name: str) -> Optional[date]:
    """Return the YYYY/MM/DD partition date embedded in a blob path, or None."""
    match = _DATE_PATH_RE.search(blob_name)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def _split_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got {gs_uri!r}")
    parsed = urlparse(gs_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if not bucket:
        raise ValueError(f"Missing bucket in {gs_uri!r}")
    return bucket, prefix


class _ClosingTextIOWrapper(io.TextIOWrapper):
    def __init__(self, buffer, *args, extra_close=(), **kwargs):
        super().__init__(buffer, *args, **kwargs)
        self._extra_close = tuple(extra_close)

    def close(self) -> None:
        try:
            super().close()
        finally:
            for handle in self._extra_close:
                handle.close()


def _wrap_reader(blob_name: str, raw: IO[bytes]) -> IO[str]:
    """Wrap a binary reader as UTF-8 text, decompressing gzip when applicable."""
    if blob_name.endswith(".gz"):
        return _ClosingTextIOWrapper(
            gzip.GzipFile(fileobj=raw, mode="rb"),
            encoding="utf-8",
            extra_close=(raw,),
        )
    return io.TextIOWrapper(raw, encoding="utf-8")


def _in_range(blob_name: str, date_range: Optional[Tuple[date, date]]) -> bool:
    if date_range is None:
        return True
    partition_date = _blob_partition_date(blob_name)
    # Objects with no recognizable partition date are always included, so
    # non-partitioned layouts keep the previous "read all" behaviour.
    if partition_date is None:
        return True
    return date_range[0] <= partition_date <= date_range[1]


def stream_log_handles(
    gs_prefix: str,
    date_range: Optional[Tuple[date, date]] = None,
    max_workers: int = 16,
) -> Iterator[Tuple[str, IO[str]]]:
    """Yield (blob_name, text handle) per object under a gs:// prefix; decompress .gz.

    When ``date_range`` (inclusive ``(start, end)``) is given, objects whose
    date-partitioned path falls outside the range are skipped without being
    downloaded. Objects with no recognizable partition date are always
    included, so non-partitioned layouts keep the previous "read all" behaviour.

    With ``max_workers > 1`` objects are downloaded concurrently in a bounded
    look-ahead window (``2 * max_workers`` in flight) so the network fetch of
    upcoming files overlaps parsing of the current one, while capping memory.
    Blobs are yielded in listing order. ``max_workers <= 1`` streams each object
    lazily one at a time (lowest memory, no prefetch).
    """
    if storage is None:
        raise ImportError(
            "google-cloud-storage is required for stream_log_handles. "
            "Install with `pip install -e .[gcp]`."
        )

    bucket_name, prefix = _split_gs_uri(gs_prefix)
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = (
        blob
        for blob in bucket.list_blobs(prefix=prefix)
        if not blob.name.endswith("/") and _in_range(blob.name, date_range)
    )

    if max_workers <= 1:
        for blob in blobs:
            yield blob.name, _wrap_reader(blob.name, blob.open("rb"))
        return

    def _download(blob) -> Tuple[str, bytes]:
        return blob.name, blob.download_as_bytes()

    window = max_workers * 2
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        pending: deque = deque()
        for blob in blobs:
            pending.append(executor.submit(_download, blob))
            if len(pending) < window:
                continue
            name, data = pending.popleft().result()
            yield name, _wrap_reader(name, io.BytesIO(data))
        while pending:
            name, data = pending.popleft().result()
            yield name, _wrap_reader(name, io.BytesIO(data))
