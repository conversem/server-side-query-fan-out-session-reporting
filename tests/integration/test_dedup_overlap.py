"""Verify that the composite-key dedup absorbs a 1-day cutover overlap.

Inserts the same logical request (same EdgeStartTimestamp + ClientIP + URI +
Host) once with source_provider='cloudflare' and once with
source_provider='aws_cloudfront'. After running the transform stage, the
clean table must contain exactly one row for the colliding key.

Requires: pytest -m bigquery (a real GCP project in env GCP_TEST_PROJECT).
"""

import os
import uuid
from datetime import datetime, timezone

import pytest

pytest.importorskip("google.cloud.bigquery")
from google.cloud import bigquery

pytestmark = [pytest.mark.bigquery, pytest.mark.integration]


def _ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000) * 1_000


def test_dedup_collapses_overlapping_cf_and_aws_rows():
    project = os.environ.get("GCP_TEST_PROJECT")
    if not project:
        pytest.skip("GCP_TEST_PROJECT not set")

    suite = uuid.uuid4().hex[:8]
    dataset_raw = f"test_dedup_raw_{suite}"
    dataset_report = f"test_dedup_report_{suite}"
    client = bigquery.Client(project=project)
    client.create_dataset(dataset_raw)
    client.create_dataset(dataset_report)

    try:
        from llm_bot_pipeline.config.constants import TABLE_CLEAN_BOT_REQUESTS
        from llm_bot_pipeline.pipeline.transform import LLMBotTransformer
        from llm_bot_pipeline.storage.bigquery_backend import BigQueryBackend

        backend = BigQueryBackend(
            project_id=project,
            dataset_raw=dataset_raw,
            dataset_report=dataset_report,
        )
        backend.initialize()

        ts = datetime(2026, 5, 21, 12, 0, 0, tzinfo=timezone.utc)
        base = {
            "EdgeStartTimestamp": _ns(ts),
            "ClientRequestURI": "/articles/llms",
            "ClientRequestHost": "example.com",
            "domain": "example.com",
            "ClientRequestUserAgent": "Mozilla/5.0 GPTBot/1.0",
            "ClientIP": "10.0.0.1",
            "ClientCountry": None,
            "EdgeResponseStatus": 200,
            "_bot_name": "GPTBot",
            "_bot_provider": "OpenAI",
            "_bot_category": "training",
        }
        backend.insert_raw_records(
            [
                {**base, "source_provider": "cloudflare", "RayID": "ray-abc"},
                {**base, "source_provider": "aws_cloudfront", "RayID": None},
            ]
        )

        transformer = LLMBotTransformer(
            project_id=project,
            source_dataset=dataset_raw,
            target_dataset=dataset_report,
        )
        result = transformer.transform_and_load(
            start_date=ts.date(), end_date=ts.date()
        )
        assert result.success
        assert result.rows_deduplicated == 1

        rows = list(
            backend.query(
                f"SELECT COUNT(*) AS n FROM `{project}.{dataset_report}.{TABLE_CLEAN_BOT_REQUESTS}` "
                f"WHERE request_host='example.com' AND request_uri='/articles/llms'"
            )
        )
        assert rows[0]["n"] == 1
    finally:
        client.delete_dataset(dataset_raw, delete_contents=True, not_found_ok=True)
        client.delete_dataset(dataset_report, delete_contents=True, not_found_ok=True)
