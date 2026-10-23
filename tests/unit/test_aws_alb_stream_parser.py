"""Unit tests for parse_alb_stream: parser-level extraction of ALBAdapter logic.

The function must accept any text IO (incl. io.StringIO from a decompressed
gzip blob), iterate lines, and yield IngestionRecord — matching what
ALBAdapter._parse_alb_line produces today but without touching the adapter's
file-based API.
"""

import io

import pytest

from llm_bot_pipeline.aws.alb_stream_parser import parse_alb_stream

# A minimal ALB access-log line. Field positions are defined in
# ingestion.providers.aws_alb.adapter.ALBAdapter.FIELD_POSITIONS.
SAMPLE_LINE = (
    "http 2024-01-15T12:30:45.123456Z app/my-alb/abc123 "
    "10.0.0.1:55001 10.0.1.5:80 0.001 0.020 0.030 200 200 "
    "512 1024 "
    '"GET https://example.com/path?q=1 HTTP/1.1" '
    '"Mozilla/5.0 GPTBot/1.0" '
    "ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 "
    "arn:aws:elasticloadbalancing:eu-west-1:111:targetgroup/tg/abc "
    '"Root=1-abc" "-" "-" 0 2024-01-15T12:30:45.000000Z "forward" "-" "-" '
    '10.0.1.5:80 200 "-" "-" TID_abc'
)


def test_parses_single_line():
    records = list(parse_alb_stream(io.StringIO(SAMPLE_LINE)))
    assert len(records) == 1
    r = records[0]
    assert r.client_ip == "10.0.0.1"
    assert r.host == "example.com"
    assert r.path == "/path"
    assert r.query_string == "q=1"
    assert r.method == "GET"
    assert r.status_code == 200
    assert "GPTBot" in r.user_agent
    assert r.timestamp.tzinfo is not None


def test_skips_empty_and_malformed_lines():
    text = f"\n{SAMPLE_LINE}\nshort line\n{SAMPLE_LINE}\n"
    records = list(parse_alb_stream(io.StringIO(text)))
    assert len(records) == 2


def test_handles_empty_handle():
    assert list(parse_alb_stream(io.StringIO(""))) == []


def test_works_with_iterable_of_lines():
    """Streamers may yield a generator of lines instead of a file-like."""
    lines = [SAMPLE_LINE, "", SAMPLE_LINE]
    records = list(parse_alb_stream(iter(lines)))
    assert len(records) == 2
