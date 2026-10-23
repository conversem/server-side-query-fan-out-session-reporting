from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from llm_bot_pipeline.pipeline.daily import auto_backfill as auto_mod
from llm_bot_pipeline.pipeline.daily.auto_backfill import (
    DayRunPolicy,
    build_day_policy,
    run_auto_backfill,
    run_auto_backfill_pipeline,
    should_use_auto_backfill,
)
from llm_bot_pipeline.pipeline.daily.cli import parse_daily_args
from llm_bot_pipeline.pipeline.daily.missing_days import (
    REASON_EMPTY_SOURCE,
    REASON_FAILED_TRANSFORM,
    MissingDay,
)

_DAY_POLICY = DayRunPolicy(
    skip_ingestion=False,
    skip_sitemap=False,
    only_domains=None,
    skip_quality=False,
    track_freshness=True,
    emit_failure_alert=True,
)


def _should(mode="gcp_bq", **overrides):
    kwargs = {
        "explicit_date": False,
        "explicit_start_date": False,
        "days": 1,
        "no_auto_backfill": False,
        "enabled": True,
    }
    kwargs.update(overrides)
    return should_use_auto_backfill(mode, **kwargs)


@contextmanager
def _patch_auto(list_missing=None, run_fn=None):
    if list_missing is None:

        def list_missing(**kwargs):
            return []

    if run_fn is None:
        run_fn = MagicMock(return_value={"success": True})
    with (
        patch.object(auto_mod, "list_missing_days", list_missing),
        patch.object(auto_mod, "run_auto_backfill", run_fn),
    ):
        yield run_fn


def test_continues_after_day_failure():
    calls = []

    def daily_runner(**kwargs):
        calls.append(kwargs["start_date"])
        if kwargs["start_date"] == date(2026, 8, 11):
            return {"success": False, "error": "ingest failed"}
        return {"success": True}

    result = run_auto_backfill(
        dates=[date(2026, 8, 13), date(2026, 8, 11), date(2026, 8, 9)],
        daily_runner=daily_runner,
        shutdown_requested=lambda: False,
        yesterday=date(2026, 8, 13),
        time_budget_seconds=2400,
        elapsed_seconds=lambda: 0,
        day_policy=lambda day: _DAY_POLICY,
    )
    assert calls == [date(2026, 8, 13), date(2026, 8, 11), date(2026, 8, 9)]
    assert result["success"] is False
    assert result["days_failed"] == 1
    assert result["days_processed"] == 2
    assert result["start_date"] == "2026-08-09"
    assert result["end_date"] == "2026-08-13"


def test_budget_skips_unstarted_days_without_failing_if_yesterday_ok():
    calls = []

    def daily_runner(**kwargs):
        calls.append(kwargs["start_date"])
        return {"success": True}

    elapsed = {"n": 0}

    def runner_and_advance(**kwargs):
        result = daily_runner(**kwargs)
        elapsed["n"] = 3000
        return result

    result = run_auto_backfill(
        dates=[date(2026, 8, 13), date(2026, 8, 9)],
        daily_runner=runner_and_advance,
        shutdown_requested=lambda: False,
        yesterday=date(2026, 8, 13),
        time_budget_seconds=2400,
        elapsed_seconds=lambda: elapsed["n"],
        day_policy=lambda day: _DAY_POLICY,
    )
    assert calls == [date(2026, 8, 13)]
    assert result["success"] is True
    assert result["days_skipped_budget"] == 1
    assert result["days_skipped_shutdown"] == 0
    assert result["leftover_dates"] == ["2026-08-09"]


def test_yesterday_failure_is_job_failure():
    result = run_auto_backfill(
        dates=[date(2026, 8, 13)],
        daily_runner=lambda **kwargs: {"success": False, "error": "boom"},
        shutdown_requested=lambda: False,
        yesterday=date(2026, 8, 13),
        time_budget_seconds=2400,
        elapsed_seconds=lambda: 0,
        day_policy=lambda day: _DAY_POLICY,
    )
    assert result["success"] is False


def test_sigterm_records_leftovers_as_shutdown_not_budget():
    def daily_runner(**kwargs):
        return {"success": True}

    stopped = {"n": 0}

    def shutdown_requested():
        stopped["n"] += 1
        return stopped["n"] >= 1

    result = run_auto_backfill(
        dates=[date(2026, 8, 13), date(2026, 8, 9)],
        daily_runner=daily_runner,
        shutdown_requested=shutdown_requested,
        yesterday=date(2026, 8, 13),
        time_budget_seconds=2400,
        elapsed_seconds=lambda: 0,
        day_policy=lambda day: _DAY_POLICY,
    )
    assert result["leftover_dates"] == ["2026-08-09"]
    assert result["days_skipped_shutdown"] == 1
    assert result["days_skipped_budget"] == 0


def test_keeps_first_day_error():
    def daily_runner(**kwargs):
        if kwargs["start_date"] == date(2026, 8, 13):
            return {"success": False, "error": "yesterday boom"}
        return {"success": False, "error": "hole boom"}

    result = run_auto_backfill(
        dates=[date(2026, 8, 13), date(2026, 8, 11)],
        daily_runner=daily_runner,
        shutdown_requested=lambda: False,
        yesterday=date(2026, 8, 13),
        time_budget_seconds=2400,
        elapsed_seconds=lambda: 0,
        day_policy=lambda day: _DAY_POLICY,
    )
    assert result["error"] == "yesterday boom"
    assert result["errors"] == [
        {"date": "2026-08-13", "error": "yesterday boom"},
        {"date": "2026-08-11", "error": "hole boom"},
    ]


def test_forwards_explicit_day_policy_fields():
    captured = []

    def daily_runner(**kwargs):
        captured.append(kwargs)
        return {"success": True}

    extra = DayRunPolicy(
        only_domains=["example.com"],
        skip_quality=True,
        track_freshness=False,
        emit_failure_alert=True,
        skip_ingestion=True,
        skip_sitemap=True,
    )

    result = run_auto_backfill(
        dates=[date(2026, 8, 13)],
        daily_runner=daily_runner,
        shutdown_requested=lambda: False,
        yesterday=date(2026, 8, 13),
        time_budget_seconds=2400,
        elapsed_seconds=lambda: 0,
        day_policy=lambda day: extra,
    )
    assert result["success"] is True
    assert captured[0]["only_domains"] == ["example.com"]
    assert captured[0]["skip_quality"] is True
    assert captured[0]["track_freshness"] is False
    assert captured[0]["emit_failure_alert"] is True
    assert captured[0]["skip_ingestion"] is True
    assert captured[0]["skip_sitemap"] is True


def _missing(day, domain, reason):
    return MissingDay(
        day=day,
        domain=domain,
        raw_count=0 if reason == REASON_EMPTY_SOURCE else 200,
        clean_count=0,
        reason=reason,
    )


def test_build_day_policy_yesterday_full_ingest():
    yesterday = date(2026, 8, 13)
    policy = build_day_policy(yesterday, yesterday, [])
    assert policy.skip_sitemap is False
    assert policy.only_domains is None
    assert policy.skip_ingestion is False
    assert policy.skip_quality is True
    assert policy.track_freshness is True
    assert policy.emit_failure_alert is True


def test_build_day_policy_failed_transform_only():
    yesterday = date(2026, 8, 13)
    hole = date(2026, 8, 11)
    missing = [_missing(hole, "www.example.com", REASON_FAILED_TRANSFORM)]
    policy = build_day_policy(hole, yesterday, missing)
    assert policy.skip_ingestion is True
    assert policy.skip_sitemap is True
    assert policy.track_freshness is False
    assert policy.emit_failure_alert is False
    assert policy.skip_quality is True


def test_build_day_policy_empty_source_limits_domains():
    yesterday = date(2026, 8, 13)
    hole = date(2026, 8, 10)
    missing = [_missing(hole, "www.example.com", REASON_EMPTY_SOURCE)]
    policy = build_day_policy(hole, yesterday, missing)
    assert policy.only_domains == ["www.example.com"]
    assert policy.skip_sitemap is True
    assert policy.skip_quality is True
    assert policy.track_freshness is False
    assert policy.emit_failure_alert is False


def test_build_day_policy_mixed_hole_ingests_empty_source_only():
    yesterday = date(2026, 8, 13)
    hole = date(2026, 8, 10)
    missing = [
        _missing(hole, "www.example.com", REASON_EMPTY_SOURCE),
        _missing(hole, "other.example.com", REASON_FAILED_TRANSFORM),
    ]
    policy = build_day_policy(hole, yesterday, missing)
    assert policy.only_domains == ["www.example.com"]
    assert policy.skip_ingestion is False
    assert policy.skip_sitemap is True
    assert policy.track_freshness is False
    assert policy.emit_failure_alert is False


def _auto_settings(lookback_days=7, domains=None):
    settings = MagicMock()
    settings.auto_backfill.lookback_days = lookback_days
    settings.auto_backfill.min_raw_rows = 100
    settings.auto_backfill.time_budget_seconds = 2400
    settings.auto_backfill.respect_cloudflare_retention = True
    settings.dataset_raw = "raw"
    settings.dataset_report = "report"
    settings.gcp_location = "EU"
    if domains is None:
        settings.domains = [MagicMock(domain="www.example.com", provider="cloudflare")]
    else:
        settings.domains = domains
    return settings


def _pipeline_kwargs(**overrides):
    kwargs = {
        "yesterday": date(2026, 8, 13),
        "settings": _auto_settings(),
        "config": {"gcp": {"project_id": "p", "service_account_key_path": "k"}},
        "daily_runner": lambda **kw: {"success": True},
        "resolve_creds": lambda config: ("p", "k"),
        "get_backend": lambda **kw: MagicMock(),
        "shutdown_requested": lambda: False,
        "run_quality_stage": lambda *args, **kw: None,
    }
    kwargs.update(overrides)
    return kwargs


def test_run_auto_backfill_pipeline_lookback_start_respects_cloudflare_floor(
    caplog,
):
    yesterday = date(2026, 8, 13)
    captured = {}

    def list_missing_days(**kwargs):
        captured["start_date"] = kwargs["start_date"]
        captured["end_date"] = kwargs["end_date"]
        return [
            _missing(date(2026, 8, 7), "www.example.com", REASON_EMPTY_SOURCE),
            _missing(date(2026, 8, 9), "www.example.com", REASON_EMPTY_SOURCE),
        ]

    class _Clock:
        @staticmethod
        def now(tz=None):
            return datetime(2026, 8, 14, 4, 0, tzinfo=timezone.utc)

    with (
        patch.object(auto_mod, "datetime", _Clock),
        caplog.at_level("INFO"),
        _patch_auto(list_missing=list_missing_days) as mock_run,
    ):
        run_auto_backfill_pipeline(**_pipeline_kwargs(yesterday=yesterday))
    assert captured["start_date"] == date(2026, 8, 8)
    assert captured["end_date"] == date(2026, 8, 13)
    assert mock_run.call_args[1]["dates"] == [date(2026, 8, 13), date(2026, 8, 9)]
    assert any(
        "window_start" in rec.message and "end_date" in rec.message
        for rec in caplog.records
        if rec.levelname == "INFO"
    )


def test_discovery_runtime_error_still_runs_yesterday():
    yesterday = date(2026, 8, 13)

    def boom(**kwargs):
        raise RuntimeError("bq unavailable")

    with _patch_auto(list_missing=boom) as mock_run:
        run_auto_backfill_pipeline(**_pipeline_kwargs(yesterday=yesterday))
    mock_run.assert_called_once()
    assert mock_run.call_args[1]["dates"] == [yesterday]


def test_dry_run_reaches_run_auto_backfill():
    with _patch_auto() as mock_run:
        run_auto_backfill_pipeline(**_pipeline_kwargs(dry_run=True))
    assert mock_run.call_args[1]["dry_run"] is True


def test_discovery_never_initializes_schema():
    backend = MagicMock()
    with _patch_auto():
        run_auto_backfill_pipeline(
            **_pipeline_kwargs(get_backend=lambda **kwargs: backend)
        )
    backend.initialize.assert_not_called()


def test_empty_domains_runs_yesterday_only():
    yesterday = date(2026, 8, 13)
    mock_run = MagicMock(return_value={"success": True})
    get_backend = MagicMock()
    with patch.object(auto_mod, "run_auto_backfill", mock_run):
        run_auto_backfill_pipeline(
            **_pipeline_kwargs(
                yesterday=yesterday,
                settings=_auto_settings(domains=[]),
                get_backend=get_backend,
            )
        )
    get_backend.assert_not_called()
    mock_run.assert_called_once()
    assert mock_run.call_args[1]["dates"] == [yesterday]


def test_close_error_does_not_replace_discovery_fallback():
    yesterday = date(2026, 8, 13)
    backend = MagicMock()
    backend.close.side_effect = RuntimeError("close failed")

    def boom(**kwargs):
        raise RuntimeError("bq unavailable")

    with _patch_auto(list_missing=boom) as mock_run:
        run_auto_backfill_pipeline(
            **_pipeline_kwargs(
                yesterday=yesterday,
                get_backend=lambda **kwargs: backend,
            )
        )
    mock_run.assert_called_once()
    assert mock_run.call_args[1]["dates"] == [yesterday]


def test_retention_value_error_is_skip_not_failure():
    def daily_runner(**kwargs):
        if kwargs["start_date"] == date(2026, 8, 9):
            raise ValueError("Start time exceeds 7-day retention limit")
        return {"success": True}

    result = run_auto_backfill(
        dates=[date(2026, 8, 13), date(2026, 8, 9)],
        daily_runner=daily_runner,
        shutdown_requested=lambda: False,
        yesterday=date(2026, 8, 13),
        time_budget_seconds=2400,
        elapsed_seconds=lambda: 0,
        day_policy=lambda day: _DAY_POLICY,
    )
    assert result["success"] is True
    assert result["days_failed"] == 0
    assert result["days_processed"] == 1
    assert result["days_skipped_retention"] == 1


def test_retention_error_message_in_result_is_skip_not_failure():
    result = run_auto_backfill(
        dates=[date(2026, 8, 13), date(2026, 8, 9)],
        daily_runner=lambda **kwargs: (
            {"success": False, "error": "Start time exceeds 7-day retention limit"}
            if kwargs["start_date"] == date(2026, 8, 9)
            else {"success": True}
        ),
        shutdown_requested=lambda: False,
        yesterday=date(2026, 8, 13),
        time_budget_seconds=2400,
        elapsed_seconds=lambda: 0,
        day_policy=lambda day: _DAY_POLICY,
    )
    assert result["success"] is True
    assert result["days_failed"] == 0
    assert result["days_skipped_retention"] == 1


def test_retention_substring_alone_is_not_a_skip():
    result = run_auto_backfill(
        dates=[date(2026, 8, 13), date(2026, 8, 9)],
        daily_runner=lambda **kwargs: (
            {"success": False, "error": "Retention is not turned on"}
            if kwargs["start_date"] == date(2026, 8, 9)
            else {"success": True}
        ),
        shutdown_requested=lambda: False,
        yesterday=date(2026, 8, 13),
        time_budget_seconds=2400,
        elapsed_seconds=lambda: 0,
        day_policy=lambda day: _DAY_POLICY,
    )
    assert result["success"] is False
    assert result["days_failed"] == 1
    assert result["error"] == "Retention is not turned on"


def test_should_use_auto_backfill_unflagged_gcp_bq():
    assert _should() is True


def test_should_use_auto_backfill_days_one():
    assert _should(days=1) is True


def test_should_use_auto_backfill_explicit_date():
    assert _should(explicit_date=True) is False


def test_should_use_auto_backfill_start_date():
    assert _should(explicit_start_date=True) is False


def test_should_use_auto_backfill_days_seven():
    assert _should(days=7) is False


def test_should_use_auto_backfill_no_auto_flag():
    assert _should(no_auto_backfill=True) is False


def test_should_use_auto_backfill_disabled():
    assert _should(enabled=False) is False


def test_cli_no_auto_backfill_flag():
    with patch("sys.argv", ["run_daily_etl", "--no-auto-backfill"]):
        args = parse_daily_args(Path("/tmp"))
    assert args.no_auto_backfill is True


def test_quality_runs_once_for_yesterday_after_loop():
    yesterday = date(2026, 8, 13)
    quality = MagicMock()
    with _patch_auto():
        run_auto_backfill_pipeline(
            **_pipeline_kwargs(yesterday=yesterday, run_quality_stage=quality)
        )
    quality.assert_called_once()
    assert quality.call_args[0][1] == yesterday
