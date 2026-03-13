"""Unit tests for sitemap parser (normalize, parse helpers)."""

from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.sitemap.parser import (
    SitemapEntry,
    _parse_urlset,
    fetch_sitemap,
    normalize_lastmod,
    normalize_url_path,
)

URLSET_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/page-a</loc>
    <lastmod>2025-01-15</lastmod>
  </url>
  <url>
    <loc>https://example.com/page-b/</loc>
    <lastmod>2025-02</lastmod>
  </url>
  <url>
    <loc>https://example.com/page-c</loc>
  </url>
</urlset>
"""

INDEX_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://example.com/sitemap-1.xml</loc>
  </sitemap>
</sitemapindex>
"""


class TestNormalizeUrlPath:
    def test_strips_scheme_and_host(self):
        assert normalize_url_path("https://example.com/foo/bar") == "/foo/bar"

    def test_preserves_trailing_slash(self):
        assert normalize_url_path("https://example.com/foo/") == "/foo/"

    def test_root_url(self):
        assert normalize_url_path("https://example.com") == "/"

    def test_lowercases(self):
        assert normalize_url_path("https://example.com/Foo/BAR") == "/foo/bar"


class TestNormalizeLastmod:
    def test_full_date(self):
        lastmod, month = normalize_lastmod("2025-01-15")
        assert lastmod == "2025-01-15"
        assert month == "2025-01"

    def test_datetime(self):
        lastmod, month = normalize_lastmod("2025-01-15T10:30:00Z")
        assert lastmod == "2025-01-15"
        assert month == "2025-01"

    def test_month_only(self):
        lastmod, month = normalize_lastmod("2025-02")
        assert lastmod is None
        assert month == "2025-02"

    def test_none(self):
        assert normalize_lastmod(None) == (None, None)

    def test_empty_string(self):
        assert normalize_lastmod("") == (None, None)

    def test_invalid(self):
        assert normalize_lastmod("not-a-date") == (None, None)

    def test_whitespace_stripped(self):
        lastmod, month = normalize_lastmod("  2025-03-01  ")
        assert lastmod == "2025-03-01"


class TestParseUrlset:
    def test_parses_entries(self):
        from defusedxml.ElementTree import fromstring

        root = fromstring(URLSET_XML)
        entries = _parse_urlset(root, "https://example.com/sitemap.xml")

        assert len(entries) == 3
        assert entries[0].url == "https://example.com/page-a"
        assert entries[0].url_path == "/page-a"
        assert entries[0].lastmod == "2025-01-15"
        assert entries[0].lastmod_month == "2025-01"

    def test_month_only_lastmod(self):
        from defusedxml.ElementTree import fromstring

        root = fromstring(URLSET_XML)
        entries = _parse_urlset(root, "src")
        page_b = entries[1]
        assert page_b.lastmod is None
        assert page_b.lastmod_month == "2025-02"

    def test_missing_lastmod(self):
        from defusedxml.ElementTree import fromstring

        root = fromstring(URLSET_XML)
        entries = _parse_urlset(root, "src")
        page_c = entries[2]
        assert page_c.lastmod is None
        assert page_c.lastmod_month is None


class TestFetchSitemap:
    def test_max_depth_returns_empty(self):
        result = fetch_sitemap("https://example.com/sitemap.xml", _depth=4)
        assert result == []

    @patch("llm_bot_pipeline.sitemap.parser.requests.get")
    def test_urlset(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.content = URLSET_XML.encode()
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        entries = fetch_sitemap("https://example.com/sitemap.xml")
        assert len(entries) == 3
        assert entries[0].sitemap_source == "https://example.com/sitemap.xml"

    @patch("llm_bot_pipeline.sitemap.parser.requests.get")
    def test_index_recurses(self, mock_get):
        mock_index_resp = MagicMock()
        mock_index_resp.content = INDEX_XML.encode()
        mock_index_resp.raise_for_status = MagicMock()

        mock_child_resp = MagicMock()
        mock_child_resp.content = URLSET_XML.encode()
        mock_child_resp.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_index_resp, mock_child_resp]

        entries = fetch_sitemap("https://example.com/sitemapindex.xml")
        assert len(entries) == 3
        assert mock_get.call_count == 2

    @patch("llm_bot_pipeline.sitemap.parser.requests.get")
    def test_http_error_returns_empty(self, mock_get):
        import requests

        mock_get.side_effect = requests.RequestException("timeout")

        entries = fetch_sitemap("https://example.com/sitemap.xml")
        assert entries == []

    @patch("llm_bot_pipeline.sitemap.parser.requests.get")
    def test_invalid_xml_returns_empty(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.content = b"not xml at all"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        entries = fetch_sitemap("https://example.com/sitemap.xml")
        assert entries == []

    @patch("llm_bot_pipeline.sitemap.parser.requests.get")
    def test_unknown_root_returns_empty(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.content = b"<root><child/></root>"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        entries = fetch_sitemap("https://example.com/sitemap.xml")
        assert entries == []
