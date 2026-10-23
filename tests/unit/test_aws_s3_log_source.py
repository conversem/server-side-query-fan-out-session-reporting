"""Unit tests for pull_aws_logs_for_date_range with a mocked GCS streamer.

The pull function composes parser + streamer + mapper. Tests verify the wiring
across all three for both CloudFront and ALB source types.
"""

import io
from datetime import date
from unittest.mock import patch

import pytest

from llm_bot_pipeline.aws.s3_log_source import (
    pull_aws_logs_for_date_range,
    pull_shared_aws_logs,
)
from llm_bot_pipeline.config.settings import (
    AWSSourceConfig,
    DomainConfig,
    Settings,
)


def _make_cloudfront_domain() -> DomainConfig:
    return DomainConfig(
        domain="example.com",
        zone_id="",
        provider="aws_s3",
        aws=AWSSourceConfig(
            source_type="cloudfront",
            s3_bucket="client-logs",
            s3_prefixes=["cloudfront/dist-123/"],
            gcs_staging_prefix="gs://staging/example/cloudfront/",
            reader_role_arn="arn:aws:iam::111122223333:role/log-reader",
        ),
    )


def _make_alb_domain() -> DomainConfig:
    return DomainConfig(
        domain="example.com",
        zone_id="",
        provider="aws_s3",
        aws=AWSSourceConfig(
            source_type="alb",
            s3_bucket="client-logs",
            s3_prefixes=["alb/api/"],
            gcs_staging_prefix="gs://staging/example/alb/",
            reader_role_arn="arn:aws:iam::111122223333:role/log-reader",
        ),
    )


CLOUDFRONT_W3C_SAMPLE = (
    "#Version: 1.0\n"
    "#Fields: date time c-ip cs-method cs(Host) cs-uri-stem cs-uri-query "
    "sc-status cs(User-Agent) sc-bytes\n"
    "2026-05-21\t12:30:45\t10.0.0.1\tGET\texample.com\t/articles/llms\t-\t"
    "200\tMozilla/5.0%20GPTBot/1.0\t1024\n"
)

ALB_SAMPLE = (
    "http 2026-05-21T12:30:45.123456Z app/my-alb/abc 10.0.0.1:55001 "
    "10.0.1.5:80 0.001 0.020 0.030 200 200 512 1024 "
    '"GET https://example.com/api HTTP/1.1" "Mozilla/5.0 GPTBot/1.0" '
    "ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 "
    "arn:aws:elasticloadbalancing:eu-west-1:111:targetgroup/tg/abc "
    '"Root=1-abc" "-" "-" 0 2026-05-21T12:30:45.000000Z "forward" "-" "-" '
    '10.0.1.5:80 200 "-" "-" TID_abc\n'
)


def _fake_stream(payload: str):
    def _streamer(gs_prefix: str, date_range=None, max_workers=16):
        yield "fake-blob.gz", io.StringIO(payload)

    return _streamer


def test_cloudfront_path_yields_raw_dicts():
    with patch(
        "llm_bot_pipeline.aws.s3_log_source.stream_log_handles",
        side_effect=_fake_stream(CLOUDFRONT_W3C_SAMPLE),
    ):
        rows = list(
            pull_aws_logs_for_date_range(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_config=_make_cloudfront_domain(),
                settings=Settings(),
                filter_llm_bots=True,
            )
        )
    assert len(rows) == 1
    row = rows[0]
    assert row["source_provider"] == "aws_cloudfront"
    assert row["domain"] == "example.com"
    assert row["ClientRequestURI"] == "/articles/llms"
    assert row["_bot_name"] == "GPTBot"


def test_alb_path_yields_raw_dicts():
    with patch(
        "llm_bot_pipeline.aws.s3_log_source.stream_log_handles",
        side_effect=_fake_stream(ALB_SAMPLE),
    ):
        rows = list(
            pull_aws_logs_for_date_range(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_config=_make_alb_domain(),
                settings=Settings(),
                filter_llm_bots=True,
            )
        )
    assert len(rows) == 1
    assert rows[0]["source_provider"] == "aws_alb"
    assert rows[0]["_bot_name"] == "GPTBot"


def test_filters_records_outside_requested_date_range():
    payload = (
        CLOUDFRONT_W3C_SAMPLE
        + "2026-05-22\t12:30:45\t10.0.0.2\tGET\texample.com\t/articles/other\t-\t"
        "200\tMozilla/5.0%20GPTBot/1.0\t1024\n"
    )
    stream_calls = []

    def _streamer(gs_prefix: str, date_range=None, max_workers=16):
        stream_calls.append(gs_prefix)
        yield "fake-blob.gz", io.StringIO(payload)

    with patch(
        "llm_bot_pipeline.aws.s3_log_source.stream_log_handles",
        side_effect=_streamer,
    ):
        rows = list(
            pull_aws_logs_for_date_range(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_config=_make_cloudfront_domain(),
                settings=Settings(),
                filter_llm_bots=True,
            )
        )

    assert len(rows) == 1
    assert rows[0]["ClientIP"] == "10.0.0.1"
    assert stream_calls == ["gs://staging/example/cloudfront/"]


def test_non_bot_rows_dropped_when_filter_enabled():
    non_bot = CLOUDFRONT_W3C_SAMPLE.replace(
        "Mozilla/5.0%20GPTBot/1.0", "Mozilla/5.0%20Chrome/120"
    )
    with patch(
        "llm_bot_pipeline.aws.s3_log_source.stream_log_handles",
        side_effect=_fake_stream(non_bot),
    ):
        rows = list(
            pull_aws_logs_for_date_range(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_config=_make_cloudfront_domain(),
                settings=Settings(),
                filter_llm_bots=True,
            )
        )
    assert rows == []


def test_filter_disabled_yields_all_rows():
    non_bot = CLOUDFRONT_W3C_SAMPLE.replace(
        "Mozilla/5.0%20GPTBot/1.0", "Mozilla/5.0%20Chrome/120"
    )
    with patch(
        "llm_bot_pipeline.aws.s3_log_source.stream_log_handles",
        side_effect=_fake_stream(non_bot),
    ):
        rows = list(
            pull_aws_logs_for_date_range(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_config=_make_cloudfront_domain(),
                settings=Settings(),
                filter_llm_bots=False,
            )
        )
    assert len(rows) == 1
    assert rows[0]["_bot_name"] is None


def _domain_on_prefix(domain: str, prefix: str) -> DomainConfig:
    return DomainConfig(
        domain=domain,
        zone_id="",
        provider="aws_s3",
        aws=AWSSourceConfig(
            source_type="alb",
            s3_bucket="client-logs",
            s3_prefixes=["alb/shared/"],
            gcs_staging_prefix=prefix,
            reader_role_arn="arn:aws:iam::111122223333:role/log-reader",
        ),
    )


def test_shared_prefix_is_downloaded_once_per_domain_group():
    stream_calls = []

    def _streamer(gs_prefix: str, date_range=None, max_workers=16):
        stream_calls.append(gs_prefix)
        yield "fake-blob.gz", io.StringIO(ALB_SAMPLE)

    domains = [
        _domain_on_prefix("one.example", "gs://staging/shared/alb/"),
        _domain_on_prefix("two.example", "gs://staging/shared/alb/"),
    ]
    with patch(
        "llm_bot_pipeline.aws.s3_log_source.stream_log_handles",
        side_effect=_streamer,
    ):
        rows = list(
            pull_shared_aws_logs(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_configs=domains,
                settings=Settings(),
            )
        )

    assert stream_calls == ["gs://staging/shared/alb/"]
    assert [row["domain"] for row in rows] == ["one.example", "two.example"]


def test_shared_pull_rejects_different_prefixes():
    first = _domain_on_prefix("one.example", "gs://staging/shared/alb/")
    second = _domain_on_prefix("two.example", "gs://staging/other/alb/")
    with pytest.raises(ValueError, match="same source_type and gcs_staging_prefix"):
        list(
            pull_shared_aws_logs(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_configs=[first, second],
                settings=Settings(),
            )
        )


def test_rejects_non_aws_domain_config():
    cf_dc = DomainConfig(domain="example.com", zone_id="z", provider="cloudflare")
    with pytest.raises(ValueError, match="aws_s3"):
        list(
            pull_aws_logs_for_date_range(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_config=cf_dc,
                settings=Settings(),
            )
        )
