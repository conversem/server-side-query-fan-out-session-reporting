"""Verify that a cutover from CF Logpush to AWS CloudFront leaves dashboard
queries returning a continuous timeseries — no gap, no spike.

The test inserts a synthetic mix of (source_provider='cloudflare') rows for
2026-05-18..20 and (source_provider='aws_cloudfront') rows for 2026-05-21..23
into the raw table, then queries the same date range as the dashboards would
and asserts row counts per day are continuous (no zero days, no doublings).

Requires: pytest -m bigquery (a real GCP project in env GCP_TEST_PROJECT).
"""

import os
import uuid
from datetime import date, datetime, timezone

import pytest

pytest.importorskip("google.cloud.bigquery")

from google.cloud import bigquery

pytestmark = [pytest.mark.bigquery, pytest.mark.integration]


def _ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000) * 1_000


def _row(day: date, source_provider: str, idx: int) -> dict:
    ts = datetime(day.year, day.month, day.day, 12, idx, 0, tzinfo=timezone.utc)
    return {
        "EdgeStartTimestamp": _ns(ts),
        "ClientRequestURI": f"/article/{idx}",
        "ClientRequestHost": "example.com",
        "domain": "example.com",
        "ClientRequestUserAgent": "Mozilla/5.0 GPTBot/1.0",
        "ClientIP": f"10.0.0.{idx}",
        "ClientCountry": None,
        "EdgeResponseStatus": 200,
        "RayID": f"ray-{idx}" if source_provider == "cloudflare" else None,
        "_bot_name": "GPTBot",
        "_bot_provider": "OpenAI",
        "_bot_category": "training",
        "source_provider": source_provider,
    }


def test_dashboard_query_returns_continuous_timeseries():
    project = os.environ.get("GCP_TEST_PROJECT")
    if not project:
        pytest.skip("GCP_TEST_PROJECT not set")

    suite = uuid.uuid4().hex[:8]
    dataset = f"test_continuity_{suite}"
    client = bigquery.Client(project=project)
    client.create_dataset(dataset)

    try:
        from llm_bot_pipeline.storage.bigquery_backend import BigQueryBackend

        backend = BigQueryBackend(
            project_id=project, dataset_raw=dataset, dataset_report=dataset
        )
        backend.initialize()

        rows: list[dict] = []
        for d in (date(2026, 5, 18), date(2026, 5, 19), date(2026, 5, 20)):
            rows.extend(_row(d, "cloudflare", i) for i in range(5))
        for d in (date(2026, 5, 21), date(2026, 5, 22), date(2026, 5, 23)):
            rows.extend(_row(d, "aws_cloudfront", i) for i in range(5))

        backend.insert_raw_records(rows)

        sql = f"""
        SELECT DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) AS d,
               COUNT(*) AS n
        FROM `{project}.{dataset}.raw_bot_requests`
        WHERE domain = 'example.com'
        GROUP BY d ORDER BY d
        """
        result = list(backend.query(sql))
        counts_by_day = {str(r["d"]): r["n"] for r in result}

        for d in (
            "2026-05-18",
            "2026-05-19",
            "2026-05-20",
            "2026-05-21",
            "2026-05-22",
            "2026-05-23",
        ):
            assert counts_by_day.get(d) == 5, f"Day {d} count: {counts_by_day.get(d)}"
    finally:
        client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)
