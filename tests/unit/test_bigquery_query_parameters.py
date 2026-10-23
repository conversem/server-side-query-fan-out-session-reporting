from datetime import date
from unittest.mock import MagicMock

# Import before autouse freezegun; a late pandas import via bigquery segfaults.
from google.cloud.bigquery import ArrayQueryParameter, ScalarQueryParameter


def test_query_binds_list_as_array_and_date_as_scalar():
    from llm_bot_pipeline.storage.bigquery_backend import BigQueryBackend

    captured = {}

    class _Job:
        def result(self):
            return []

    client = MagicMock()

    def _query(sql, job_config=None):
        captured["params"] = list(job_config.query_parameters)
        return _Job()

    client.query.side_effect = _query
    backend = BigQueryBackend.__new__(BigQueryBackend)
    backend._get_client = lambda: client

    backend.query(
        "SELECT 1",
        params={
            "start_date": date(2026, 8, 8),
            "domains": ["www.example.com", "aws.example.com"],
        },
    )
    kinds = {p.name: type(p) for p in captured["params"]}
    assert kinds["start_date"] is ScalarQueryParameter
    assert kinds["domains"] is ArrayQueryParameter


def test_execute_binds_list_as_array():
    from llm_bot_pipeline.storage.bigquery_backend import BigQueryBackend

    captured = {}

    class _Job:
        num_dml_affected_rows = 0

        def result(self):
            return []

    client = MagicMock()

    def _query(sql, job_config=None):
        captured["params"] = list(job_config.query_parameters)
        return _Job()

    client.query.side_effect = _query
    backend = BigQueryBackend.__new__(BigQueryBackend)
    backend._get_client = lambda: client

    backend.execute(
        "SELECT 1",
        params={"domains": ["www.example.com", "aws.example.com"]},
    )
    kinds = {p.name: type(p) for p in captured["params"]}
    assert kinds["domains"] is ArrayQueryParameter
