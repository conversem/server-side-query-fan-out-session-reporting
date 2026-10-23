"""Unit tests for map_to_raw_row: IngestionRecord → CF-shaped raw dict.

The output dict feeds backend.insert_raw_records and must match the
raw_bot_requests schema (bigquery_schemas.py:30-45). EdgeStartTimestamp is
INT64 nanoseconds and REQUIRED — V1 guard fails fast on missing timestamps.
"""

from datetime import datetime, timezone

import pytest

from llm_bot_pipeline.aws.raw_row_mapper import map_to_raw_row
from llm_bot_pipeline.ingestion.base import IngestionRecord
from llm_bot_pipeline.utils.bot_classifier import BotClassification


def _make_record(**kwargs) -> IngestionRecord:
    base = dict(
        timestamp=datetime(2026, 5, 21, 12, 30, 45, 123456, tzinfo=timezone.utc),
        client_ip="10.0.0.1",
        method="GET",
        host="example.com",
        path="/articles/llms",
        status_code=200,
        user_agent="Mozilla/5.0 GPTBot/1.0",
    )
    base.update(kwargs)
    return IngestionRecord(**base)


def test_basic_mapping_with_bot_info():
    bot = BotClassification(
        bot_name="GPTBot", bot_provider="OpenAI", bot_category="training"
    )
    row = map_to_raw_row(
        _make_record(),
        domain="example.com",
        source_provider="aws_cloudfront",
        bot_info=bot,
    )
    assert row["ClientRequestURI"] == "/articles/llms"
    assert row["ClientRequestHost"] == "example.com"
    assert row["domain"] == "example.com"
    assert row["ClientIP"] == "10.0.0.1"
    assert row["EdgeResponseStatus"] == 200
    assert row["ClientRequestUserAgent"] == "Mozilla/5.0 GPTBot/1.0"
    assert row["source_provider"] == "aws_cloudfront"
    assert row["_bot_name"] == "GPTBot"
    assert row["_bot_provider"] == "OpenAI"
    assert row["_bot_category"] == "training"
    assert row["RayID"] is None
    assert row["ClientCountry"] is None


def test_appends_query_string_to_uri():
    row = map_to_raw_row(
        _make_record(path="/search", query_string="q=llm&page=2"),
        domain="example.com",
        source_provider="aws_cloudfront",
        bot_info=None,
    )
    assert row["ClientRequestURI"] == "/search?q=llm&page=2"


def test_empty_path_becomes_root():
    row = map_to_raw_row(
        _make_record(path=""),
        domain="example.com",
        source_provider="aws_cloudfront",
        bot_info=None,
    )
    assert row["ClientRequestURI"] == "/"


def test_edge_start_timestamp_is_nanoseconds():
    """EdgeStartTimestamp must be INT64 ns (transform.py uses DIV(.,1000))."""
    ts = datetime(2026, 5, 21, 12, 30, 45, 123456, tzinfo=timezone.utc)
    row = map_to_raw_row(
        _make_record(timestamp=ts),
        domain="example.com",
        source_provider="aws_cloudfront",
        bot_info=None,
    )
    expected_us = int(ts.timestamp() * 1_000_000)
    assert row["EdgeStartTimestamp"] == expected_us * 1_000


def test_missing_timestamp_raises_v1_guard():
    """V1: EdgeStartTimestamp is REQUIRED in bigquery_schemas.py — fail fast."""
    rec = _make_record()
    rec.timestamp = None  # bypass dataclass check for test
    with pytest.raises(ValueError, match="timestamp"):
        map_to_raw_row(
            rec,
            domain="example.com",
            source_provider="aws_cloudfront",
            bot_info=None,
        )


def test_bot_info_none_populates_nulls():
    row = map_to_raw_row(
        _make_record(),
        domain="example.com",
        source_provider="aws_alb",
        bot_info=None,
    )
    assert row["_bot_name"] is None
    assert row["_bot_provider"] is None
    assert row["_bot_category"] is None


def test_source_provider_is_pass_through():
    """The caller decides aws_cloudfront vs aws_alb; mapper does not infer."""
    row = map_to_raw_row(
        _make_record(),
        domain="example.com",
        source_provider="aws_alb",
        bot_info=None,
    )
    assert row["source_provider"] == "aws_alb"
