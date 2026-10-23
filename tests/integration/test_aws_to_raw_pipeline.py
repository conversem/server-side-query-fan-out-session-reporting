"""End-to-end integration test for the AWS S3 log source.

Mocks the GCS streamer to return our committed gzip fixtures, then drives
pull_aws_logs_for_date_range to verify the full chain — parser → classifier →
mapper — produces correctly shaped raw rows.

Pure-Python; no GCP project required.
"""

import gzip
import io
from datetime import date
from pathlib import Path
from unittest.mock import patch

from llm_bot_pipeline.aws.s3_log_source import pull_aws_logs_for_date_range
from llm_bot_pipeline.config.settings import (
    AWSSourceConfig,
    DomainConfig,
    Settings,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _fake_streamer_from(path: Path):
    def _streamer(gs_prefix: str, date_range=None, max_workers=16):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            text = fh.read()
        yield path.name, io.StringIO(text)

    return _streamer


def _cf_domain() -> DomainConfig:
    return DomainConfig(
        domain="example.com",
        zone_id="",
        provider="aws_s3",
        aws=AWSSourceConfig(
            source_type="cloudfront",
            s3_bucket="b",
            s3_prefixes=["cloudfront/dist-123/"],
            gcs_staging_prefix="gs://staging/example/cloudfront/",
            reader_role_arn="arn:aws:iam::111122223333:role/r",
        ),
    )


def _alb_domain() -> DomainConfig:
    return DomainConfig(
        domain="example.com",
        zone_id="",
        provider="aws_s3",
        aws=AWSSourceConfig(
            source_type="alb",
            s3_bucket="b",
            s3_prefixes=["alb/api/"],
            gcs_staging_prefix="gs://staging/example/alb/",
            reader_role_arn="arn:aws:iam::111122223333:role/r",
        ),
    )


def test_cloudfront_fixture_yields_two_bot_rows():
    with patch(
        "llm_bot_pipeline.aws.s3_log_source.stream_log_handles",
        side_effect=_fake_streamer_from(FIXTURE_DIR / "cloudfront_sample.gz"),
    ):
        rows = list(
            pull_aws_logs_for_date_range(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_config=_cf_domain(),
                settings=Settings(),
            )
        )
    bot_names = sorted(r["_bot_name"] for r in rows)
    assert bot_names == ["ClaudeBot", "GPTBot"]
    for row in rows:
        assert row["source_provider"] == "aws_cloudfront"
        assert row["domain"] == "example.com"
        assert isinstance(row["EdgeStartTimestamp"], int)
        assert row["EdgeStartTimestamp"] > 0


def test_alb_fixture_yields_two_bot_rows():
    with patch(
        "llm_bot_pipeline.aws.s3_log_source.stream_log_handles",
        side_effect=_fake_streamer_from(FIXTURE_DIR / "alb_sample.log.gz"),
    ):
        rows = list(
            pull_aws_logs_for_date_range(
                start_date=date(2026, 5, 21),
                end_date=date(2026, 5, 21),
                domain_config=_alb_domain(),
                settings=Settings(),
            )
        )
    bot_names = sorted(r["_bot_name"] for r in rows)
    assert bot_names == ["ClaudeBot", "GPTBot"]
    assert all(r["source_provider"] == "aws_alb" for r in rows)
