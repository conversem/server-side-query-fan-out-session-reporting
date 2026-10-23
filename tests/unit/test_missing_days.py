from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from llm_bot_pipeline.cloudflare.logpull import RETENTION_DAYS
from llm_bot_pipeline.pipeline.daily.missing_days import (
    REASON_EMPTY_SOURCE,
    REASON_FAILED_TRANSFORM,
    MissingDay,
    effective_lookback_days,
    has_cloudflare_domains,
    list_missing_days,
    nightly_window_start,
    plan_nightly_dates,
    schedule_nightly_dates,
)


def _backend(rows):
    backend = MagicMock()
    backend.get_full_table_id.side_effect = lambda t: f"proj.ds.{t}"
    backend.query.return_value = rows
    return backend


def test_empty_source_is_missing():
    backend = _backend(
        [
            {
                "d": date(2026, 8, 10),
                "domain": "www.example.com",
                "raw_count": 0,
                "clean_count": 0,
                "reason": REASON_EMPTY_SOURCE,
            }
        ]
    )
    gaps = list_missing_days(
        backend,
        domains=["www.example.com"],
        start_date=date(2026, 8, 8),
        end_date=date(2026, 8, 13),
    )
    assert gaps[0].day == date(2026, 8, 10)
    assert gaps[0].reason == REASON_EMPTY_SOURCE


def test_failed_transform_is_missing():
    backend = _backend(
        [
            {
                "d": date(2026, 8, 11),
                "domain": "www.example.com",
                "raw_count": 23189,
                "clean_count": 0,
                "reason": REASON_FAILED_TRANSFORM,
            }
        ]
    )
    gaps = list_missing_days(
        backend,
        domains=["www.example.com"],
        start_date=date(2026, 8, 8),
        end_date=date(2026, 8, 13),
    )
    assert gaps[0].reason == REASON_FAILED_TRANSFORM
    assert gaps[0].raw_count == 23189


def test_query_uses_params_and_missing_predicates():
    backend = _backend([])
    captured = {}

    def _capture(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    backend.query.side_effect = _capture
    list_missing_days(
        backend,
        domains=["www.example.com", "aws.example.com"],
        start_date=date(2026, 8, 8),
        end_date=date(2026, 8, 13),
        min_raw_rows=100,
    )
    sql = captured["sql"]
    assert "GENERATE_DATE_ARRAY" in sql
    assert "www.example.com" not in sql
    assert "raw_count = 0 OR raw_count >= @min_raw_rows" in " ".join(sql.split())
    assert captured["params"]["start_date"] == date(2026, 8, 8)
    assert captured["params"]["end_date"] == date(2026, 8, 13)
    assert captured["params"]["domains"] == ["www.example.com", "aws.example.com"]
    assert captured["params"]["host_suffixes"] == ["", ""]
    assert captured["params"]["min_raw_rows"] == 100


def test_query_uses_host_suffix_for_catch_all():
    backend = _backend([])
    captured = {}

    def _capture(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    backend.query.side_effect = _capture
    list_missing_days(
        backend,
        domains=["www.example.com"],
        start_date=date(2026, 8, 8),
        end_date=date(2026, 8, 13),
        host_suffixes=["example.com"],
    )
    sql = captured["sql"]
    assert "ENDS_WITH" in sql
    assert "ClientRequestHost" in sql
    assert "request_host" in sql
    assert captured["params"]["host_suffixes"] == ["example.com"]


def test_host_suffixes_length_must_match_domains():
    backend = _backend([])
    with pytest.raises(ValueError, match="host_suffixes"):
        list_missing_days(
            backend,
            domains=["www.example.com"],
            start_date=date(2026, 8, 8),
            end_date=date(2026, 8, 13),
            host_suffixes=["a", "b"],
        )


def test_empty_domain_list_returns_empty_without_query():
    backend = _backend([])
    gaps = list_missing_days(
        backend,
        domains=[],
        start_date=date(2026, 8, 8),
        end_date=date(2026, 8, 13),
    )
    assert gaps == []
    backend.query.assert_not_called()


def test_clamp_lookback_when_cloudflare_retention_on():
    assert effective_lookback_days(30, has_cloudflare=True) == RETENTION_DAYS
    assert effective_lookback_days(3, has_cloudflare=True) == 3
    assert effective_lookback_days(30, has_cloudflare=False) == 30
    assert (
        effective_lookback_days(
            30, has_cloudflare=True, respect_cloudflare_retention=False
        )
        == 30
    )


def test_effective_lookback_days_logs_warning_when_clamped(caplog):
    with caplog.at_level("WARNING"):
        effective_lookback_days(30, has_cloudflare=True)
    assert any(
        "clamped to Cloudflare retention" in record.message
        for record in caplog.records
        if record.levelname == "WARNING"
    )


def test_yesterday_first_then_oldest_missing():
    yesterday = date(2026, 8, 13)
    missing = [
        MissingDay(yesterday, "www.example.com", 0, 0, REASON_EMPTY_SOURCE),
        MissingDay(date(2026, 8, 11), "www.example.com", 0, 0, REASON_EMPTY_SOURCE),
        MissingDay(date(2026, 8, 9), "www.example.com", 0, 0, REASON_EMPTY_SOURCE),
        MissingDay(date(2026, 8, 9), "other.example.com", 0, 0, REASON_EMPTY_SOURCE),
    ]
    ordered = schedule_nightly_dates(yesterday, missing, lookback_days=7)
    assert ordered[0] == yesterday
    assert ordered[1:] == [date(2026, 8, 9), date(2026, 8, 11)]


def test_yesterday_always_included_when_complete():
    yesterday = date(2026, 8, 13)
    ordered = schedule_nightly_dates(yesterday, missing=[], lookback_days=7)
    assert ordered == [yesterday]


def test_missing_outside_lookback_ignored():
    yesterday = date(2026, 8, 13)
    missing = [
        MissingDay(date(2026, 8, 1), "www.example.com", 0, 0, REASON_EMPTY_SOURCE),
    ]
    ordered = schedule_nightly_dates(yesterday, missing, lookback_days=7)
    assert date(2026, 8, 1) not in ordered
    assert ordered == [yesterday]


def test_effective_lookback_days_rejects_below_one():
    with pytest.raises(ValueError, match="lookback_days must be >= 1"):
        effective_lookback_days(0, has_cloudflare=False)


def test_has_cloudflare_domains_checks_provider():
    assert has_cloudflare_domains([SimpleNamespace(provider="cloudflare")]) is True
    assert has_cloudflare_domains([SimpleNamespace(provider="aws_s3")]) is False
    assert has_cloudflare_domains([]) is False


def test_plan_nightly_dates_applies_floor_to_holes_only():
    yesterday = date(2026, 8, 13)
    missing = [
        MissingDay(date(2026, 8, 7), "www.example.com", 0, 0, REASON_EMPTY_SOURCE),
        MissingDay(date(2026, 8, 9), "www.example.com", 0, 0, REASON_EMPTY_SOURCE),
    ]
    ordered = plan_nightly_dates(yesterday, missing, date(2026, 8, 8))
    assert ordered[0] == yesterday
    assert date(2026, 8, 7) not in ordered
    assert ordered[1:] == [date(2026, 8, 9)]


def test_nightly_window_start_is_max_of_calendar_and_floor():
    yesterday = date(2026, 8, 13)
    now = datetime(2026, 8, 14, 4, 0, tzinfo=timezone.utc)
    assert nightly_window_start(
        yesterday, now, lookback=7, has_cloudflare=True
    ) == date(2026, 8, 8)
    assert nightly_window_start(
        yesterday, now, lookback=7, has_cloudflare=True, respect=False
    ) == date(2026, 8, 7)


def test_naive_now_utc_treated_as_utc():
    naive = datetime(2026, 8, 14, 0, 30)  # noqa: DTZ001
    aware = datetime(2026, 8, 14, 0, 30, tzinfo=timezone.utc)
    yesterday = date(2026, 8, 13)
    assert nightly_window_start(
        yesterday, naive, lookback=7, has_cloudflare=True
    ) == nightly_window_start(yesterday, aware, lookback=7, has_cloudflare=True)
    assert nightly_window_start(
        yesterday, naive, lookback=7, has_cloudflare=True
    ) == date(2026, 8, 8)
