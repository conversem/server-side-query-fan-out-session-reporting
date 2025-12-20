"""
Unit tests for url_utils module.

Tests URL session name derivation functionality.
"""

import pytest

from llm_bot_pipeline.utils.url_utils import derive_session_name


class TestDeriveSessionName:
    """Tests for derive_session_name function."""

    def test_basic_path_with_hyphens(self):
        """Path with hyphens should be converted to spaces."""
        url = "example.nl/blog/home-buying-guide"
        assert derive_session_name(url) == "home buying guide"

    def test_path_with_underscores(self):
        """Path with underscores should be converted to spaces."""
        url = "example.nl/blog/post_name"
        assert derive_session_name(url) == "post name"

    def test_path_with_mixed_separators(self):
        """Path with both hyphens and underscores should be converted."""
        url = "example.nl/blog/first-time_buyer-checklist"
        assert derive_session_name(url) == "first time buyer checklist"

    def test_simple_path_segment(self):
        """Simple path segment should work."""
        url = "example.nl/mortgage/calculator"
        assert derive_session_name(url) == "calculator"

    def test_multiple_path_segments(self):
        """Should extract last segment from multi-level path."""
        url = "example.nl/category/subcategory/item-name"
        assert derive_session_name(url) == "item name"

    def test_path_with_file_extension(self):
        """File extensions should be removed."""
        url = "example.nl/article.pdf"
        assert derive_session_name(url) == "article"

    def test_path_with_html_extension(self):
        """HTML extensions should be removed."""
        url = "example.nl/blog/post_name.html"
        assert derive_session_name(url) == "post name"

    def test_path_with_extension_and_hyphens(self):
        """Extensions and hyphens should both be handled."""
        url = "example.nl/blog/home-buying-guide.pdf"
        assert derive_session_name(url) == "home buying guide"

    def test_homepage_root_url(self):
        """Root URL should return 'homepage'."""
        url = "example.nl/"
        assert derive_session_name(url) == "homepage"

    def test_homepage_no_path(self):
        """URL with no path should return 'homepage'."""
        url = "example.nl"
        assert derive_session_name(url) == "homepage"

    def test_homepage_with_scheme(self):
        """Homepage URL with scheme should return 'homepage'."""
        url = "https://example.nl/"
        assert derive_session_name(url) == "homepage"

    def test_url_with_scheme_and_path(self):
        """Full URL with scheme should work correctly."""
        url = "https://example.nl/blog/home-buying-guide"
        assert derive_session_name(url) == "home buying guide"

    def test_url_with_query_params(self):
        """Query parameters should be ignored."""
        url = "example.nl/blog/home-buying-guide?utm_source=test"
        assert derive_session_name(url) == "home buying guide"

    def test_url_with_fragment(self):
        """URL fragments should be ignored."""
        url = "example.nl/blog/home-buying-guide#section"
        assert derive_session_name(url) == "home buying guide"

    def test_url_with_trailing_slash(self):
        """Trailing slash should be handled correctly."""
        url = "example.nl/blog/home-buying-guide/"
        assert derive_session_name(url) == "home buying guide"

    def test_multiple_dots_in_filename(self):
        """Multiple dots should only remove last extension."""
        url = "example.nl/blog/file.name.html"
        assert derive_session_name(url) == "file name"

    def test_dot_in_middle_of_name(self):
        """Dot in middle of name should be preserved."""
        url = "example.nl/blog/v2.0-guide"
        assert derive_session_name(url) == "v2 0 guide"

    def test_empty_path_after_stripping(self):
        """Empty path after processing should return 'unknown'."""
        # This is an edge case - a path segment that's just dots or special chars
        url = "example.nl/..."
        result = derive_session_name(url)
        # After removing dots, we get empty string, should return "unknown"
        assert result == "unknown"

    def test_single_character_segment(self):
        """Single character segment should work."""
        url = "example.nl/a"
        assert derive_session_name(url) == "a"

    def test_segment_with_only_spaces_after_processing(self):
        """Segment that becomes only spaces should return 'unknown'."""
        url = "example.nl/---"
        result = derive_session_name(url)
        # After replacing hyphens with spaces and stripping, should be empty
        assert result == "unknown"

    def test_numeric_segment(self):
        """Numeric segment should work."""
        url = "example.nl/blog/2024"
        assert derive_session_name(url) == "2024"

    def test_segment_with_mixed_case(self):
        """Mixed case should be preserved."""
        url = "example.nl/blog/HomeBuyingGuide"
        assert derive_session_name(url) == "HomeBuyingGuide"

    def test_prd_example_1(self):
        """Test example from PRD."""
        url = "example.nl/blog/home-buying-guide"
        assert derive_session_name(url) == "home buying guide"

    def test_prd_example_2(self):
        """Test example from PRD."""
        url = "example.nl/mortgage/calculator"
        assert derive_session_name(url) == "calculator"

    def test_prd_example_3(self):
        """Test example from PRD."""
        url = "example.nl/tips/first-time-buyer-checklist"
        assert derive_session_name(url) == "first time buyer checklist"

    def test_prd_example_4(self):
        """Test example from PRD."""
        url = "example.nl/"
        assert derive_session_name(url) == "homepage"

    def test_url_with_non_http_scheme(self):
        """URLs with non-HTTP schemes should still work."""
        # Even though we add https:// for URLs without schemes,
        # URLs with other schemes should parse correctly
        url = "ftp://example.com/file.pdf"
        # The path extraction should still work
        result = derive_session_name(url)
        assert result == "file"

    def test_empty_string_url(self):
        """Empty string should be handled gracefully."""
        url = ""
        # Should handle gracefully, likely returns "homepage" or "unknown"
        result = derive_session_name(url)
        assert result in ("homepage", "unknown")
