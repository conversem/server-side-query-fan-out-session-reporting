"""Tests for UrlFilteringSettings."""

import os
from unittest.mock import patch

import pytest

from llm_bot_pipeline.config.settings import Settings, UrlFilteringSettings


class TestUrlFilteringSettingsDefaults:
    def test_enabled_by_default(self):
        s = UrlFilteringSettings()
        assert s.enabled is True

    def test_default_drop_extensions_contains_js(self):
        s = UrlFilteringSettings()
        assert "js" in s.drop_extensions
        assert "css" in s.drop_extensions

    def test_default_image_extensions_contains_jpg(self):
        s = UrlFilteringSettings()
        assert "jpg" in s.image_extensions
        assert "png" in s.image_extensions

    def test_default_drop_path_prefixes(self):
        s = UrlFilteringSettings()
        assert "/assets/js/" in s.drop_path_prefixes


class TestUrlFilteringSettingsFromDict:
    def test_from_empty_dict_uses_defaults(self):
        s = UrlFilteringSettings.from_dict({})
        assert s.enabled is True
        assert "js" in s.drop_extensions

    def test_from_dict_overrides(self):
        s = UrlFilteringSettings.from_dict(
            {
                "enabled": False,
                "drop_extensions": ["ts", "tsx"],
                "image_extensions": ["tiff"],
                "drop_path_prefixes": ["/custom/"],
            }
        )
        assert s.enabled is False
        assert s.drop_extensions == frozenset({"ts", "tsx"})
        assert s.image_extensions == frozenset({"tiff"})
        assert s.drop_path_prefixes == ("/custom/",)

    def test_from_dict_partial_override(self):
        s = UrlFilteringSettings.from_dict({"enabled": False})
        assert s.enabled is False
        assert "js" in s.drop_extensions


class TestUrlFilteringSettingsFromEnv:
    @patch.dict(os.environ, {"URL_FILTERING_ENABLED": "false"})
    def test_enabled_from_env(self):
        s = UrlFilteringSettings.from_env()
        assert s.enabled is False

    @patch.dict(os.environ, {"URL_FILTERING_DROP_EXTENSIONS": "ts,tsx"})
    def test_drop_extensions_from_env(self):
        s = UrlFilteringSettings.from_env()
        assert s.drop_extensions == frozenset({"ts", "tsx"})

    @patch.dict(os.environ, {"URL_FILTERING_DROP_PATH_PREFIXES": "/a/,/b/"})
    def test_drop_path_prefixes_from_env(self):
        s = UrlFilteringSettings.from_env()
        assert s.drop_path_prefixes == ("/a/", "/b/")


class TestUrlFilteringSettingsValidation:
    def test_valid_settings_no_errors(self):
        s = UrlFilteringSettings()
        assert s.validate() == []

    def test_overlap_extension_warning(self):
        s = UrlFilteringSettings(
            drop_extensions=frozenset({"jpg"}),
            image_extensions=frozenset({"jpg"}),
        )
        errors = s.validate()
        assert any("overlap" in e.lower() for e in errors)


class TestUrlFilteringSettingsRoundTrip:
    def test_to_dict_and_back(self):
        original = UrlFilteringSettings()
        d = original.to_dict()
        restored = UrlFilteringSettings.from_dict(d)
        assert restored.enabled == original.enabled
        assert restored.drop_extensions == original.drop_extensions
        assert restored.image_extensions == original.image_extensions
        assert restored.drop_path_prefixes == original.drop_path_prefixes


class TestSettingsIntegration:
    def test_settings_has_url_filtering(self):
        s = Settings()
        assert isinstance(s.url_filtering, UrlFilteringSettings)
        assert s.url_filtering.enabled is True

    def test_settings_from_dict_parses_url_filtering(self):
        config = {"url_filtering": {"enabled": False}}
        s = Settings.from_dict(config)
        assert s.url_filtering.enabled is False

    def test_settings_validate_includes_url_filtering(self):
        s = Settings()
        errors = s.validate()
        url_errors = [
            e for e in errors if "url_filtering" in e.lower() or "overlap" in e.lower()
        ]
        assert url_errors == []
