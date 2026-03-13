"""Unit tests for the SQL compatibility layer."""

from datetime import date

import pytest

from llm_bot_pipeline.pipeline.sql_compat import (
    SQLBuilder,
    bot_pattern_match,
    coalesce_bool,
    countif,
    current_timestamp,
    date_filter,
    date_from_raw_timestamp,
    date_from_timestamp,
    day_of_week,
    day_of_week_from_raw,
    extract_hour,
    extract_hour_from_raw,
    json_array_unnest,
    response_status_category,
    row_number_dedup,
    string_agg,
    table_reference,
    timestamp_from_raw,
    url_path_depth,
    url_path_extract,
)


class TestCurrentTimestamp:
    def test_sqlite(self):
        assert current_timestamp("sqlite") == "datetime('now')"

    def test_bigquery(self):
        assert current_timestamp("bigquery") == "CURRENT_TIMESTAMP()"


class TestTimestampFromRaw:
    def test_sqlite_passthrough(self):
        assert timestamp_from_raw("ts", "sqlite") == "ts"

    def test_bigquery_conversion(self):
        result = timestamp_from_raw("ts", "bigquery")
        assert "TIMESTAMP_MICROS" in result
        assert "DIV(ts, 1000)" in result


class TestDateFromTimestamp:
    def test_sqlite(self):
        assert date_from_timestamp("col", "sqlite") == "date(col)"

    def test_bigquery(self):
        assert date_from_timestamp("col", "bigquery") == "DATE(col)"


class TestDateFromRawTimestamp:
    def test_sqlite(self):
        result = date_from_raw_timestamp("ts", "sqlite")
        assert "date(ts)" == result

    def test_bigquery(self):
        result = date_from_raw_timestamp("ts", "bigquery")
        assert "DATE(" in result
        assert "TIMESTAMP_MICROS" in result


class TestExtractHour:
    def test_sqlite(self):
        result = extract_hour("ts", "sqlite")
        assert "strftime('%H'" in result

    def test_bigquery(self):
        result = extract_hour("ts", "bigquery")
        assert "EXTRACT(HOUR FROM ts)" == result


class TestExtractHourFromRaw:
    def test_sqlite_passthrough(self):
        result = extract_hour_from_raw("ts", "sqlite")
        assert "strftime" in result

    def test_bigquery_wraps(self):
        result = extract_hour_from_raw("ts", "bigquery")
        assert "EXTRACT(HOUR FROM" in result
        assert "TIMESTAMP_MICROS" in result


class TestDayOfWeek:
    def test_sqlite_has_case(self):
        result = day_of_week("dt", "sqlite")
        assert "Sunday" in result
        assert "strftime" in result

    def test_bigquery(self):
        result = day_of_week("dt", "bigquery")
        assert "FORMAT_DATE('%A'" in result


class TestDayOfWeekFromRaw:
    def test_sqlite(self):
        result = day_of_week_from_raw("ts", "sqlite")
        assert "Sunday" in result

    def test_bigquery(self):
        result = day_of_week_from_raw("ts", "bigquery")
        assert "FORMAT_DATE" in result


class TestDateFilter:
    def test_sqlite(self):
        result = date_filter("col", date(2025, 1, 1), date(2025, 1, 31), "sqlite")
        assert "date(col) >= '2025-01-01'" in result
        assert "date(col) <= '2025-01-31'" in result

    def test_bigquery(self):
        result = date_filter("col", date(2025, 1, 1), date(2025, 1, 31), "bigquery")
        assert "DATE(col) >= '2025-01-01'" in result


class TestCountif:
    def test_sqlite(self):
        result = countif("x > 5", "sqlite")
        assert "SUM(CASE WHEN x > 5 THEN 1 ELSE 0 END)" == result

    def test_bigquery(self):
        result = countif("x > 5", "bigquery")
        assert "COUNTIF(x > 5)" == result


class TestCoalesceBool:
    def test_sqlite_true(self):
        result = coalesce_bool("col", True, "sqlite")
        assert result == "COALESCE(col, 1)"

    def test_sqlite_false(self):
        result = coalesce_bool("col", False, "sqlite")
        assert result == "COALESCE(col, 0)"

    def test_bigquery_true(self):
        result = coalesce_bool("col", True, "bigquery")
        assert "TRUE" in result

    def test_bigquery_false(self):
        result = coalesce_bool("col", False, "bigquery")
        assert "FALSE" in result


class TestTableReference:
    def test_sqlite_strips_prefix(self):
        assert table_reference("project.dataset.table", "sqlite") == "table"

    def test_bigquery_backticks(self):
        result = table_reference("project.dataset.table", "bigquery")
        assert result == "`project.dataset.table`"


class TestUrlPathExtract:
    def test_sqlite_has_case(self):
        result = url_path_extract("uri", "sqlite")
        assert "INSTR" in result

    def test_bigquery_uses_regex(self):
        result = url_path_extract("uri", "bigquery")
        assert "REGEXP_EXTRACT" in result


class TestUrlPathDepth:
    def test_sqlite(self):
        result = url_path_depth("uri", "sqlite")
        assert "LENGTH" in result

    def test_bigquery(self):
        result = url_path_depth("uri", "bigquery")
        assert "ARRAY_LENGTH" in result


class TestBotPatternMatch:
    def test_sqlite_uses_like(self):
        result = bot_pattern_match("ua", "GPTBot", "sqlite")
        assert "LIKE '%GPTBot%'" in result

    def test_bigquery_uses_regex(self):
        result = bot_pattern_match("ua", "GPTBot", "bigquery")
        assert "REGEXP_CONTAINS" in result


class TestStringAgg:
    def test_sqlite_basic(self):
        result = string_agg("col", ", ", False, "sqlite")
        assert result == "GROUP_CONCAT(col, ', ')"

    def test_sqlite_distinct(self):
        result = string_agg("col", ", ", True, "sqlite")
        assert "DISTINCT" in result

    def test_bigquery_basic(self):
        result = string_agg("col", ", ", False, "bigquery")
        assert "STRING_AGG(col, ', ')" == result

    def test_bigquery_distinct_ordered(self):
        result = string_agg("col", ", ", True, "bigquery", order_by="col ASC")
        assert "DISTINCT" in result
        assert "ORDER BY col ASC" in result


class TestRowNumberDedup:
    def test_generates_correct_sql(self):
        result = row_number_dedup(["a", "b"], "c DESC", "sqlite")
        assert "ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY c DESC)" == result


class TestJsonArrayUnnest:
    def test_sqlite(self):
        from_clause, value_expr = json_array_unnest("t", "col", "sqlite")
        assert from_clause == "t, json_each(t.col)"
        assert value_expr == "json_each.value"

    def test_bigquery(self):
        from_clause, value_expr = json_array_unnest("t", "col", "bigquery")
        assert "UNNEST(JSON_EXTRACT_STRING_ARRAY(t.col))" in from_clause
        assert value_expr == "j"

    def test_custom_alias(self):
        from_clause, value_expr = json_array_unnest(
            "t", "col", "bigquery", alias="elem"
        )
        assert "AS elem" in from_clause
        assert value_expr == "elem"


class TestResponseStatusCategory:
    def test_has_all_categories(self):
        result = response_status_category("status", "sqlite")
        assert "2xx_success" in result
        assert "5xx_server_error" in result


class TestSQLBuilder:
    def test_builder_delegates(self):
        b = SQLBuilder("sqlite")
        assert b.current_timestamp() == "datetime('now')"
        assert b.countif("x > 0") == "SUM(CASE WHEN x > 0 THEN 1 ELSE 0 END)"

    def test_builder_bigquery(self):
        b = SQLBuilder("bigquery")
        assert b.current_timestamp() == "CURRENT_TIMESTAMP()"

    def test_builder_json_array_unnest(self):
        b = SQLBuilder("sqlite")
        from_clause, val = b.json_array_unnest("t", "urls")
        assert "json_each" in from_clause

    def test_builder_all_methods(self):
        b = SQLBuilder("sqlite")
        assert b.timestamp_from_raw("ts") == "ts"
        assert "date(" in b.date_from_timestamp("ts")
        assert "date(" in b.date_from_raw_timestamp("ts")
        assert "strftime" in b.extract_hour("ts")
        assert "strftime" in b.extract_hour_from_raw("ts")
        assert "Sunday" in b.day_of_week("dt")
        assert "Sunday" in b.day_of_week_from_raw("ts")
        assert "2025-01-01" in b.date_filter("col", date(2025, 1, 1), date(2025, 1, 2))
        assert "2025-01-01" in b.date_filter_raw(
            "ts", date(2025, 1, 1), date(2025, 1, 2)
        )
        assert "COALESCE" in b.coalesce_bool("col", False)
        assert "table" == b.table_ref("ds.table")
        assert "INSTR" in b.url_path("uri")
        assert "LENGTH" in b.url_depth("uri")
        assert "LIKE" in b.bot_match("ua", "Bot")
        assert "2xx_success" in b.status_category("code")
        assert "ROW_NUMBER" in b.row_number(["a"], "b")
