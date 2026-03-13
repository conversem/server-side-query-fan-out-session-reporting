"""Unit tests for storage factory functions."""

import pytest

from llm_bot_pipeline.storage.base import StorageError
from llm_bot_pipeline.storage.factory import (
    get_backend,
    is_backend_available,
    list_available_backends,
    register_backend,
)


class TestGetBackend:
    def test_sqlite_explicit(self, tmp_path):
        backend = get_backend("sqlite", db_path=tmp_path / "test.db")
        assert backend.backend_type == "sqlite"
        backend.close()

    def test_case_insensitive(self, tmp_path):
        backend = get_backend("SQLite", db_path=tmp_path / "test.db")
        assert backend.backend_type == "sqlite"
        backend.close()

    def test_unknown_backend_raises(self):
        with pytest.raises(StorageError, match="Unknown storage backend"):
            get_backend("postgres", db_path="/tmp/fake.db")


class TestListAvailableBackends:
    def test_includes_sqlite(self):
        backends = list_available_backends()
        assert "sqlite" in backends

    @pytest.mark.bigquery
    def test_includes_bigquery(self):
        backends = list_available_backends()
        assert "bigquery" in backends


class TestIsBackendAvailable:
    def test_sqlite_available(self):
        assert is_backend_available("sqlite") is True

    @pytest.mark.bigquery
    def test_bigquery_available(self):
        assert is_backend_available("bigquery") is True

    def test_unknown_not_available(self):
        assert is_backend_available("postgres") is False
