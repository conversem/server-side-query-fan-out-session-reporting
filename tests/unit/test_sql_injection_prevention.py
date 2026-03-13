"""Tests for SQL injection prevention: table name and date column validation."""

import pytest

from llm_bot_pipeline.config.constants import VALID_DATE_COLUMNS, VALID_TABLE_NAMES
from llm_bot_pipeline.storage.base import validate_date_column, validate_table_name


class TestValidateTableName:
    """Tests for validate_table_name()."""

    @pytest.mark.parametrize("table", sorted(VALID_TABLE_NAMES))
    def test_valid_table_names_accepted(self, table):
        assert validate_table_name(table) == table

    def test_rejects_unknown_table(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("evil_table")

    def test_rejects_sql_injection_attempt(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("bot_requests_daily; DROP TABLE users--")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("")

    def test_rejects_table_with_special_chars(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("table`name")

    def test_rejects_subquery(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("(SELECT * FROM users)")


class TestValidateDateColumn:
    """Tests for validate_date_column()."""

    @pytest.mark.parametrize("col", sorted(VALID_DATE_COLUMNS))
    def test_valid_date_columns_accepted(self, col):
        assert validate_date_column(col) == col

    def test_rejects_unknown_column(self):
        with pytest.raises(ValueError, match="Invalid date column"):
            validate_date_column("malicious_column")

    def test_rejects_sql_injection_in_date_column(self):
        with pytest.raises(ValueError, match="Invalid date column"):
            validate_date_column("request_date; DROP TABLE--")


class TestValidTableNamesCompleteness:
    """Verify VALID_TABLE_NAMES covers all TABLE_* constants."""

    def test_all_table_constants_included(self):
        from llm_bot_pipeline.config import constants

        table_constants = {
            v
            for k, v in vars(constants).items()
            if k.startswith("TABLE_") and isinstance(v, str)
        }
        assert (
            table_constants <= VALID_TABLE_NAMES
        ), f"Missing from VALID_TABLE_NAMES: {table_constants - VALID_TABLE_NAMES}"
