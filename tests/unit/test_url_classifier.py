"""Tests for URL resource type classifier."""

import pytest

from llm_bot_pipeline.config.settings import UrlFilteringSettings
from llm_bot_pipeline.utils.url_classifier import classify_url


@pytest.fixture
def default_settings():
    return UrlFilteringSettings()


@pytest.fixture
def disabled_settings():
    return UrlFilteringSettings(enabled=False)


class TestDropExtensions:
    @pytest.mark.parametrize(
        "path",
        [
            "/assets/js/chunks-es/Table.880558c.js",
            "/script.js",
            "/deep/path/module.mjs",
            "/styles/main.css",
            "/fonts/roboto.woff2",
            "/fonts/icon.woff",
            "/fonts/arial.ttf",
            "/fonts/open-sans.eot",
            "/fonts/mono.otf",
            "/main.js.map",
            "/favicon.ico",
        ],
    )
    def test_drop_extensions(self, path, default_settings):
        assert classify_url(path, default_settings) is None


class TestImageExtensions:
    @pytest.mark.parametrize(
        "path",
        [
            "/images/solar-panel.jpg",
            "/img/hero.jpeg",
            "/photos/banner.png",
            "/media/icon.gif",
            "/logo.svg",
            "/photos/house.webp",
            "/images/chart.avif",
            "/old/diagram.bmp",
        ],
    )
    def test_image_extensions(self, path, default_settings):
        assert classify_url(path, default_settings) == "image"


class TestDocumentUrls:
    @pytest.mark.parametrize(
        "path",
        [
            "/",
            "/about",
            "/blog/my-article",
            "/page.html",
            "/docs/guide.htm",
            "/downloads/report.pdf",
            "/data/export.csv",
            "/api/v1/feed.xml",
            "/zonnepanelen/advies",
            "/isolatie/vloerisolatie",
        ],
    )
    def test_document_urls(self, path, default_settings):
        assert classify_url(path, default_settings) == "document"


class TestDropPathPrefixes:
    @pytest.mark.parametrize(
        "path",
        [
            "/assets/js/vendor/react.production.min",
            "/assets/css/main.bundle",
            "/static/media/logo",
            "/_next/data/build-id/page.json",
            "/chunks/abc123",
            "/vendor/lib/module",
            "/node_modules/pkg/index",
            "/__/firebase/init",
        ],
    )
    def test_drop_path_prefixes(self, path, default_settings):
        assert classify_url(path, default_settings) is None


class TestDisabled:
    @pytest.mark.parametrize(
        "path",
        [
            "/script.js",
            "/styles.css",
            "/assets/js/chunk.js",
            "/image.png",
            "/page.html",
        ],
    )
    def test_disabled_returns_document(self, path, disabled_settings):
        assert classify_url(path, disabled_settings) == "document"


class TestEdgeCases:
    def test_empty_path(self, default_settings):
        assert classify_url("", default_settings) == "document"

    def test_root_path(self, default_settings):
        assert classify_url("/", default_settings) == "document"

    def test_query_string_already_stripped(self, default_settings):
        assert classify_url("/page", default_settings) == "document"

    def test_extension_case_insensitive(self, default_settings):
        assert classify_url("/script.JS", default_settings) is None
        assert classify_url("/image.PNG", default_settings) == "image"

    def test_dotfile_not_confused_with_extension(self, default_settings):
        assert classify_url("/.hidden", default_settings) == "document"

    def test_path_with_dots_in_directory(self, default_settings):
        assert classify_url("/v2.0/api/endpoint", default_settings) == "document"


class TestCustomSettings:
    def test_custom_drop_extensions(self):
        s = UrlFilteringSettings(drop_extensions=frozenset({"ts", "tsx"}))
        assert classify_url("/app.ts", s) is None
        assert classify_url("/script.js", s) == "document"

    def test_custom_path_prefixes(self):
        s = UrlFilteringSettings(drop_path_prefixes=("/cdn/", "/build/"))
        assert classify_url("/cdn/lib/react", s) is None
        assert classify_url("/assets/js/chunk", s) == "document"
