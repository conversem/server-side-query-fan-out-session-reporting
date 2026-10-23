from llm_bot_pipeline.config.settings import AutoBackfillSettings, Settings, _as_bool


def test_defaults_when_block_omitted():
    settings = Settings.from_dict({"pipeline": {"processing_mode": "gcp_bq"}})
    assert settings.auto_backfill.enabled is True
    assert settings.auto_backfill.lookback_days == 7
    assert settings.auto_backfill.min_raw_rows == 100
    assert settings.auto_backfill.time_budget_seconds == 2400
    assert settings.auto_backfill.respect_cloudflare_retention is True


def test_from_dict_reads_pipeline_auto_backfill():
    settings = Settings.from_dict(
        {
            "pipeline": {
                "processing_mode": "gcp_bq",
                "auto_backfill": {
                    "enabled": False,
                    "lookback_days": 14,
                    "min_raw_rows": 50,
                    "time_budget_seconds": 1800,
                    "respect_cloudflare_retention": False,
                },
            }
        }
    )
    assert settings.auto_backfill.enabled is False
    assert settings.auto_backfill.lookback_days == 14
    assert settings.auto_backfill.time_budget_seconds == 1800


def test_from_env_gets_dataclass_defaults():
    settings = Settings.from_env()
    assert settings.auto_backfill.time_budget_seconds == 2400


def test_as_bool_accepts_env_truthy_strings():
    assert _as_bool("yes") is True
    assert _as_bool("1") is True
    assert _as_bool("true") is True
    assert _as_bool("YES") is True
    assert _as_bool("no") is False


def test_from_dict_accepts_yes_string_as_enabled():
    settings = Settings.from_dict(
        {
            "pipeline": {
                "processing_mode": "gcp_bq",
                "auto_backfill": {"enabled": "yes"},
            }
        }
    )
    assert settings.auto_backfill.enabled is True


def test_validate_rejects_non_positive_lookback():
    bad = AutoBackfillSettings(lookback_days=0)
    errors = bad.validate()
    assert any("lookback_days" in e for e in errors)
