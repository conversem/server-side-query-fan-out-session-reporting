"""Tests for --allow-raw-sql gate in export_excel.py CLI."""

import logging
from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest


def _make_args(**overrides):
    defaults = dict(
        backend=None,
        output="report.xlsx",
        tables=None,
        all=False,
        start_date=None,
        end_date=None,
        query=None,
        allow_raw_sql=False,
        sheet_name="Results",
        limit=None,
        verbose=False,
        json_logs=False,
    )
    defaults.update(overrides)
    return Namespace(**defaults)


@patch("scripts.export_excel.get_backend")
@patch("scripts.export_excel.get_settings")
@patch("scripts.export_excel.parse_args")
class TestRawSqlGate:
    def test_query_without_allow_flag_exits(
        self, mock_parse, mock_settings, mock_backend, caplog
    ):
        mock_parse.return_value = _make_args(
            query="SELECT 1",
            allow_raw_sql=False,
        )
        mock_settings.return_value = MagicMock(
            storage_backend="sqlite", backend_kwargs=lambda: {}
        )
        mock_backend.return_value = MagicMock()

        with patch("scripts.export_excel.setup_logging"):
            with caplog.at_level(logging.ERROR):
                from scripts.export_excel import main

                result = main()

        assert result == 1
        assert any("allow-raw-sql" in r.message.lower() for r in caplog.records)

    def test_query_with_allow_flag_succeeds(
        self, mock_parse, mock_settings, mock_backend
    ):
        mock_parse.return_value = _make_args(
            query="SELECT 1",
            allow_raw_sql=True,
        )
        mock_settings.return_value = MagicMock(
            storage_backend="sqlite", backend_kwargs=lambda: {}
        )
        be = MagicMock()
        be.initialize = MagicMock()
        be.close = MagicMock()
        exporter_mock = MagicMock()
        exporter_mock.export_query.return_value = "out.xlsx"
        mock_backend.return_value = be

        with patch("scripts.export_excel.ExcelExporter", return_value=exporter_mock):
            from scripts.export_excel import main

            result = main()

        assert result == 0
        exporter_mock.export_query.assert_called_once()

    def test_query_flag_logs_warning(
        self, mock_parse, mock_settings, mock_backend, caplog
    ):
        mock_parse.return_value = _make_args(
            query="SELECT 1",
            allow_raw_sql=True,
        )
        mock_settings.return_value = MagicMock(
            storage_backend="sqlite", backend_kwargs=lambda: {}
        )
        be = MagicMock()
        be.initialize = MagicMock()
        be.close = MagicMock()
        exporter_mock = MagicMock()
        exporter_mock.export_query.return_value = "out.xlsx"
        mock_backend.return_value = be

        # setup_logging removes all handlers (including caplog's); bypass it here
        with patch("scripts.export_excel.setup_logging"):
            with patch(
                "scripts.export_excel.ExcelExporter", return_value=exporter_mock
            ):
                with caplog.at_level(logging.WARNING):
                    from scripts.export_excel import main

                    main()

        assert any("raw sql" in r.message.lower() for r in caplog.records)


@patch("scripts.export_excel.get_backend")
@patch("scripts.export_excel.get_settings")
@patch("scripts.export_excel.parse_args")
class TestTablesValidation:
    """Tests for --tables validation against VALID_TABLE_NAMES whitelist."""

    def test_tables_valid(self, mock_parse, mock_settings, mock_backend):
        mock_parse.return_value = _make_args(tables="bot_requests_daily")
        mock_settings.return_value = MagicMock(
            storage_backend="sqlite", backend_kwargs=lambda: {}
        )
        be = MagicMock()
        be.initialize = MagicMock()
        be.close = MagicMock()
        exporter_mock = MagicMock()
        exporter_mock.export_report.return_value = "out.xlsx"
        mock_backend.return_value = be

        with patch("scripts.export_excel.ExcelExporter", return_value=exporter_mock):
            from scripts.export_excel import main

            result = main()

        assert result == 0
        exporter_mock.export_report.assert_called_once()
        call_kwargs = exporter_mock.export_report.call_args[1]
        assert call_kwargs["tables"] == ["bot_requests_daily"]

    def test_tables_invalid(self, mock_parse, mock_settings, mock_backend, caplog):
        mock_parse.return_value = _make_args(tables="nonexistent_table")
        mock_settings.return_value = MagicMock(
            storage_backend="sqlite", backend_kwargs=lambda: {}
        )
        mock_backend.return_value = MagicMock()

        with patch("scripts.export_excel.setup_logging"):
            with caplog.at_level(logging.ERROR):
                from scripts.export_excel import main

                result = main()

        assert result == 1
        messages = " ".join(r.message for r in caplog.records)
        assert "Invalid table" in messages
        assert "nonexistent_table" in messages

    def test_tables_injection(self, mock_parse, mock_settings, mock_backend, caplog):
        mock_parse.return_value = _make_args(tables="x; DROP TABLE users;--")
        mock_settings.return_value = MagicMock(
            storage_backend="sqlite", backend_kwargs=lambda: {}
        )
        mock_backend.return_value = MagicMock()

        with patch("scripts.export_excel.setup_logging"):
            with caplog.at_level(logging.ERROR):
                from scripts.export_excel import main

                result = main()

        assert result == 1
        messages = " ".join(r.message for r in caplog.records)
        assert "Invalid table" in messages
        assert "x; DROP" in messages
