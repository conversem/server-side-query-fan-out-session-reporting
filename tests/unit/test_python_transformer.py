"""
Unit tests for PythonTransformer and related utilities.

Tests transformation of IngestionRecord to clean dicts, deduplication,
URL path helpers, status categorization, and stats tracking.
"""

from datetime import datetime, timezone

import pytest

from llm_bot_pipeline.ingestion.base import IngestionRecord
from llm_bot_pipeline.pipeline.python_transformer import (
    _WEEKDAY_NAMES,
    PythonTransformer,
    _status_category,
    extract_url_path,
    url_path_depth,
)


def make_record(
    *,
    timestamp=None,
    client_ip="192.0.2.1",
    method="GET",
    host="example.com",
    path="/",
    status_code=200,
    user_agent="Mozilla/5.0 (compatible; GPTBot/1.0)",
    extra=None,
):
    """Create an IngestionRecord with sensible defaults."""
    if timestamp is None:
        timestamp = datetime(2025, 3, 3, 12, 0, 0, tzinfo=timezone.utc)
    return IngestionRecord(
        timestamp=timestamp,
        client_ip=client_ip,
        method=method,
        host=host,
        path=path,
        status_code=status_code,
        user_agent=user_agent,
        extra=extra or {},
    )


class TestTransformKnownBot:
    """GPTBot UA -> correct clean dict fields."""

    def test_transform_known_bot(self):
        record = make_record(
            path="/api/data",
            status_code=200,
            user_agent="Mozilla/5.0 AppleWebKit/537.36 (compatible; GPTBot/1.0)",
        )
        transformer = PythonTransformer()
        result = transformer.transform(record)

        assert result is not None
        assert result["request_timestamp"] == record.timestamp.isoformat()
        assert result["request_date"] == "2025-03-03"
        assert result["request_hour"] == 12
        assert result["day_of_week"] == "Monday"
        assert result["request_uri"] == "/api/data"
        assert result["request_host"] == "example.com"
        assert result["url_path"] == "/api/data"
        assert result["url_path_depth"] == 2
        assert result["user_agent_raw"] == record.user_agent
        assert result["bot_name"] == "GPTBot"
        assert result["bot_provider"] == "OpenAI"
        assert result["bot_category"] == "training"
        assert result["response_status"] == 200
        assert result["response_status_category"] == "2xx"
        assert "_processed_at" in result


class TestTransformUnknownBot:
    """Chrome UA filtered out."""

    def test_transform_unknown_bot_returns_none(self):
        record = make_record(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
        )
        transformer = PythonTransformer()
        result = transformer.transform(record)

        assert result is None


class TestDedup:
    """Deduplication behavior."""

    def test_dedup_same_record(self):
        record = make_record()
        transformer = PythonTransformer()

        first = transformer.transform(record)
        second = transformer.transform(record)

        assert first is not None
        assert second is None

    def test_reset_dedup(self):
        record = make_record()
        transformer = PythonTransformer()

        first = transformer.transform(record)
        transformer.reset_dedup()
        second = transformer.transform(record)

        assert first is not None
        assert second is not None


class TestUrlPathExtraction:
    """Query string and fragment stripped."""

    def test_url_path_extraction(self):
        assert extract_url_path("/foo") == "/foo"
        assert extract_url_path("/foo?q=1") == "/foo"
        assert extract_url_path("/foo#bar") == "/foo"
        assert extract_url_path("/foo?q=1&x=2#section") == "/foo"


class TestUrlPathDepth:
    """Path segment counting."""

    def test_url_path_depth(self):
        assert url_path_depth("/") == 0
        assert url_path_depth("") == 0
        assert url_path_depth("/a") == 1
        assert url_path_depth("/a/b") == 2
        assert url_path_depth("/a/b/c") == 3
        assert url_path_depth("a/b/c") == 3


class TestStatusCategory:
    """HTTP code to category mapping."""

    def test_status_category(self):
        assert _status_category(200) == "2xx"
        assert _status_category(299) == "2xx"
        assert _status_category(301) == "3xx"
        assert _status_category(404) == "4xx"
        assert _status_category(500) == "5xx"
        assert _status_category(503) == "5xx"
        assert _status_category(0) == "other"
        assert _status_category(199) == "other"
        assert _status_category(600) == "other"


class TestDayOfWeek:
    """Monday through Sunday."""

    def test_day_of_week(self):
        assert _WEEKDAY_NAMES == (
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        )

        transformer = PythonTransformer()
        # 2025-03-03 = Monday, 03-04 = Tuesday, ... 03-09 = Sunday
        for i, day in enumerate(_WEEKDAY_NAMES):
            ts = datetime(2025, 3, 3 + i, 12, 0, 0, tzinfo=timezone.utc)
            record = make_record(timestamp=ts)
            result = transformer.transform(record)
            assert result is not None
            assert result["day_of_week"] == day


class TestStatsTracking:
    """transformed/filtered/duplicates counts."""

    def test_stats_tracking(self):
        transformer = PythonTransformer()

        assert transformer.stats == {
            "transformed": 0,
            "filtered": 0,
            "duplicates": 0,
            "url_filtered": 0,
        }

        transformer.transform(make_record(user_agent="GPTBot/1.0"))
        assert transformer.stats["transformed"] == 1

        transformer.transform(
            make_record(user_agent="Mozilla/5.0 Chrome/120", client_ip="10.0.0.2")
        )
        assert transformer.stats["filtered"] == 1

        transformer.transform(make_record(user_agent="GPTBot/1.0"))
        assert transformer.stats["duplicates"] == 1

        assert transformer.stats == {
            "transformed": 1,
            "filtered": 1,
            "duplicates": 1,
            "url_filtered": 0,
        }


class TestExtraFieldsMapped:
    """ClientCountry from extra dict."""

    def test_extra_fields_mapped(self):
        record = make_record(
            user_agent="GPTBot/1.0",
            extra={
                "ClientCountry": "US",
            },
        )
        transformer = PythonTransformer()
        result = transformer.transform(record)

        assert result is not None
        assert result["crawler_country"] == "US"

    def test_extra_fields_defaults(self):
        record = make_record(user_agent="GPTBot/1.0", extra={})
        transformer = PythonTransformer()
        result = transformer.transform(record)

        assert result is not None
        assert result["crawler_country"] == ""


from llm_bot_pipeline.config.settings import UrlFilteringSettings


class TestUrlFiltering:
    """URL resource type filtering in PythonTransformer."""

    def test_js_file_is_filtered(self):
        t = PythonTransformer()
        record = make_record(path="/assets/js/chunks-es/Table.880558c.js")
        result = t.transform(record)
        assert result is None

    def test_css_file_is_filtered(self):
        t = PythonTransformer()
        record = make_record(path="/styles/main.css")
        result = t.transform(record)
        assert result is None

    def test_html_page_has_resource_type_document(self):
        t = PythonTransformer()
        record = make_record(path="/zonnepanelen/advies")
        result = t.transform(record)
        assert result is not None
        assert result["resource_type"] == "document"

    def test_image_has_resource_type_image(self):
        t = PythonTransformer()
        record = make_record(path="/images/solar-panel.jpg")
        result = t.transform(record)
        assert result is not None
        assert result["resource_type"] == "image"

    def test_url_filtered_stat_tracked(self):
        t = PythonTransformer()
        t.transform(make_record(path="/script.js"))
        t.transform(make_record(path="/style.css"))
        assert t.stats["url_filtered"] == 2

    def test_filtering_disabled_keeps_js(self):
        settings = UrlFilteringSettings(enabled=False)
        t = PythonTransformer(url_filtering_settings=settings)
        record = make_record(path="/script.js")
        result = t.transform(record)
        assert result is not None
        assert result["resource_type"] == "document"

    def test_custom_settings_respected(self):
        settings = UrlFilteringSettings(drop_extensions=frozenset({"ts"}))
        t = PythonTransformer(url_filtering_settings=settings)
        js_result = t.transform(make_record(path="/app.js"))
        ts_result = t.transform(make_record(path="/app.ts", client_ip="192.0.2.2"))
        assert js_result is not None
        assert ts_result is None
