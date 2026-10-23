"""ALB log stream → IngestionRecord iterator."""

from __future__ import annotations

from typing import IO, Iterable, Iterator, Union

from ..ingestion.base import IngestionRecord
from ..ingestion.providers.aws_alb.adapter import ALBAdapter


def parse_alb_stream(
    handle: Union[IO[str], Iterable[str]],
) -> Iterator[IngestionRecord]:
    """Yield one IngestionRecord per non-empty ALB line; skip empty or malformed lines."""
    adapter = ALBAdapter()
    yield from adapter.parse_stream(handle)
