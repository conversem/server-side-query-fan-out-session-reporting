"""BigQuery integration: initialize() re-adds missing source_provider."""

import os

import pytest

from llm_bot_pipeline.storage.bigquery_backend import BigQueryBackend

pytestmark = pytest.mark.bigquery


@pytest.fixture
def test_project() -> str:
    project = os.environ.get("GCP_TEST_PROJECT")
    if not project:
        pytest.skip("GCP_TEST_PROJECT not set")
    return project


def test_initialize_auto_migrates_source_provider(test_project):
    pid = os.getpid()
    backend = BigQueryBackend(
        project_id=test_project,
        dataset_raw=f"test_raw_migration_{pid}",
        dataset_report=f"test_report_migration_{pid}",
    )
    client = backend._get_client()

    backend.initialize()
    table_id = f"{test_project}.{backend.dataset_raw}.raw_bot_requests"
    table = client.get_table(table_id)
    assert any(f.name == "source_provider" for f in table.schema)

    table.schema = [f for f in table.schema if f.name != "source_provider"]
    client.update_table(table, ["schema"])
    table = client.get_table(table_id)
    assert not any(f.name == "source_provider" for f in table.schema)

    backend.initialize()
    table = client.get_table(table_id)
    assert any(f.name == "source_provider" for f in table.schema)

    client.delete_dataset(backend.dataset_raw, delete_contents=True, not_found_ok=True)
    client.delete_dataset(
        backend.dataset_report, delete_contents=True, not_found_ok=True
    )
