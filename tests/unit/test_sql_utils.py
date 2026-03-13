"""Tests for build_clean_insert_sql() table/column validation and value escaping."""

import pytest

from llm_bot_pipeline.config.constants import VALID_TABLE_NAMES
from llm_bot_pipeline.pipeline.sql_utils import build_clean_insert_sql


class TestBuildCleanInsertSql:
    """Tests for build_clean_insert_sql()."""

    def test_valid_table_and_columns(self):
        row = {"request_date": "2024-01-01", "hit_count": 5}
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        assert result.startswith("INSERT INTO bot_requests_daily")
        assert "request_date" in result
        assert "hit_count" in result

    def test_invalid_table_name(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            build_clean_insert_sql({"col": 1}, table="x; DROP TABLE y")

    def test_invalid_column_name(self):
        with pytest.raises(ValueError, match="Invalid column name"):
            build_clean_insert_sql({"col; --": 1}, table="bot_requests_daily")

    def test_null_and_bool_values(self):
        row = {"flag": True, "other_flag": False, "missing": None}
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        assert "1" in result
        assert "0" in result
        assert "NULL" in result

    def test_string_value_escaping(self):
        row = {"name": "it's a test"}
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        assert "it''s a test" in result

    def test_numeric_values(self):
        row = {"count": 42, "ratio": 3.14}
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        assert "42" in result
        assert "3.14" in result

    def test_unknown_table_rejected(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            build_clean_insert_sql({"col": 1}, table="nonexistent_table")

    def test_column_with_spaces_rejected(self):
        with pytest.raises(ValueError, match="Invalid column name"):
            build_clean_insert_sql({"bad column": 1}, table="bot_requests_daily")

    def test_column_starting_with_digit_rejected(self):
        with pytest.raises(ValueError, match="Invalid column name"):
            build_clean_insert_sql({"1col": 1}, table="bot_requests_daily")

    # --- Value type coverage (None, bool, int, float, str) ---

    @pytest.mark.parametrize(
        "row,expected_in,expected_not_in",
        [
            ({"col": None}, ["NULL"], ["None"]),
            (
                {"t": True, "f": False},
                ["1", "0"],
                ["True", "False"],
            ),
            ({"zero": 0, "neg": -1}, ["0", "-1"], []),
            ({"zero": 0.0, "neg": -3.14}, ["0.0", "-3.14"], []),
            ({"name": "hello"}, ["'hello'"], []),
        ],
    )
    def test_value_types(self, row, expected_in, expected_not_in):
        """Value types should be correctly serialized in SQL."""
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        for s in expected_in:
            assert s in result
        for s in expected_not_in:
            assert s not in result

    # --- SQL escaping and injection prevention ---

    @pytest.mark.parametrize(
        "row,expected_in",
        [
            ({"name": "O'Brien"}, ["O''Brien"]),
            ({"name": "a''b''c"}, ["a''''b''''c"]),
            ({"col": "x' OR '1'='1"}, ["x'' OR ''1''=''1"]),
            ({"col": "; DROP TABLE users; --"}, ["; DROP TABLE users; --"]),
            ({"col": "valid -- comment"}, ["'valid -- comment'"]),
        ],
    )
    def test_sql_escaping_and_injection_prevention(self, row, expected_in):
        """String values should be escaped; injection attempts should be literal."""
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        for s in expected_in:
            assert s in result
        assert result.startswith("INSERT INTO bot_requests_daily")

    # --- Table and column name validation ---

    @pytest.mark.parametrize("table", sorted(VALID_TABLE_NAMES))
    def test_all_valid_tables_accepted(self, table):
        result = build_clean_insert_sql({"col": 1}, table=table)
        assert f"INSERT INTO {table}" in result

    def test_column_underscore_allowed(self):
        row = {"col_name": "value"}
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        assert "col_name" in result

    def test_column_starting_with_underscore_allowed(self):
        row = {"_id": 1}
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        assert "_id" in result

    def test_column_with_mixed_case_allowed(self):
        row = {"ColName": "x"}
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        assert "ColName" in result

    @pytest.mark.parametrize(
        "bad_col", ["col-name", "col.name", "col@name", "col;drop"]
    )
    def test_column_special_chars_rejected(self, bad_col):
        """Column names with special chars should be rejected."""
        with pytest.raises(ValueError, match="Invalid column name"):
            build_clean_insert_sql({bad_col: 1}, table="bot_requests_daily")

    # --- Edge cases: special characters, Unicode, empty string ---

    @pytest.mark.parametrize(
        "row,expected_substrings",
        [
            ({"col": ""}, ["''"]),
            ({"col": "line1\nline2"}, ["line1", "line2"]),
            ({"col": "a\tb"}, ["a\tb"]),
            ({"col": "café 日本語 🎉"}, ["café", "日本語", "🎉"]),
            ({"col": "l'été"}, ["l''été"]),
        ],
    )
    def test_string_edge_cases(self, row, expected_substrings):
        """Special characters, Unicode, and empty string should be handled."""
        result = build_clean_insert_sql(row, table="bot_requests_daily")
        for s in expected_substrings:
            assert s in result
