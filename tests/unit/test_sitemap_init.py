"""Unit tests for sitemap/__init__.py pipeline functions."""

from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.sitemap import fetch_and_store_sitemaps
from llm_bot_pipeline.sitemap.parser import SitemapEntry


@pytest.fixture
def mock_backend():
    backend = MagicMock()
    backend.insert_sitemap_urls.return_value = 3
    return backend


class TestFetchAndStoreSitemaps:
    @patch("llm_bot_pipeline.sitemap.fetch_sitemap")
    def test_stores_entries(self, mock_fetch, mock_backend):
        mock_fetch.return_value = [
            SitemapEntry("/a", "/a", "2025-01-01", "2025-01", "src"),
            SitemapEntry("/b", "/b", None, None, "src"),
        ]
        result = fetch_and_store_sitemaps(["https://example.com/s.xml"], mock_backend)

        assert result["success"] is True
        assert result["urls_stored"] == 3
        mock_backend.insert_sitemap_urls.assert_called_once()

    @patch("llm_bot_pipeline.sitemap.fetch_sitemap")
    def test_empty_sitemap(self, mock_fetch, mock_backend):
        mock_fetch.return_value = []
        result = fetch_and_store_sitemaps(["https://example.com/s.xml"], mock_backend)

        assert result["success"] is True
        assert result["urls_stored"] == 0
        mock_backend.insert_sitemap_urls.assert_not_called()

    @patch("llm_bot_pipeline.sitemap.fetch_sitemap")
    def test_fetch_error_logged(self, mock_fetch, mock_backend):
        mock_fetch.side_effect = Exception("network error")
        result = fetch_and_store_sitemaps(["https://example.com/s.xml"], mock_backend)

        assert result["urls_stored"] == 0
        assert len(result["errors"]) == 1


class TestRunSitemapPipeline:
    def test_no_urls_skips(self, mock_backend):
        from llm_bot_pipeline.sitemap import run_sitemap_pipeline

        result = run_sitemap_pipeline(mock_backend, sitemap_urls=[])
        assert result["success"] is True
        assert result.get("skipped") is True
