"""Tests for monitoring view definitions."""

from scripts.monitoring_view_definitions import VIEW_DEFINITIONS, get_view_definitions


def test_view_definitions_all_present():
    """Import definitions and assert expected view names."""
    expected = {
        "v_pipeline_health",
        "v_stale_tables",
        "v_daily_traffic_summary",
        "v_weekly_traffic_trend",
        "v_bot_category_split",
        "v_provider_summary",
        "v_provider_daily",
        "v_top_content_7d",
        "v_content_by_user_request",
        "v_traffic_anomaly_check",
        "v_error_rate_check",
        "v_executive_summary_7d",
    }
    actual = set(VIEW_DEFINITIONS.keys())
    assert actual == expected


def test_view_sql_valid_syntax():
    """Basic validation that each view SQL starts with SELECT or WITH."""
    views = get_view_definitions("test-project")
    valid_starts = ("SELECT", "WITH")
    for view_name, (sql, _) in views.items():
        stripped = sql.strip().upper()
        assert stripped.startswith(
            valid_starts
        ), f"{view_name}: view SQL must start with SELECT or WITH, got: {stripped[:80]}..."
