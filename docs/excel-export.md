# Excel Export

Export pipeline data from any storage backend (SQLite or BigQuery) to `.xlsx` workbooks.

## CLI Usage

```bash
# Full report with default tables (from config-determined backend)
python scripts/export_excel.py --output report.xlsx

# Specific tables only
python scripts/export_excel.py --tables daily_summary,url_performance --output report.xlsx

# Date-filtered report
python scripts/export_excel.py --start-date 2026-01-01 --end-date 2026-01-31 --output jan-report.xlsx

# Export from BigQuery explicitly
python scripts/export_excel.py --backend bigquery --output bq-report.xlsx

# Custom SQL query
python scripts/export_excel.py --query "SELECT bot_name, COUNT(*) as cnt FROM bot_requests_daily GROUP BY bot_name" --output bots.xlsx

# Row limit per table
python scripts/export_excel.py --limit 10000 --output sample.xlsx
```

## CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--backend` | from config | `sqlite` or `bigquery` |
| `--output`, `-o` | `report.xlsx` | Output file path |
| `--tables` | all defaults | Comma-separated table names |
| `--all` | | Export all default report tables |
| `--start-date` | none | Start date filter (YYYY-MM-DD) |
| `--end-date` | none | End date filter (YYYY-MM-DD) |
| `--query` | none | Custom SQL query to export |
| `--sheet-name` | `Results` | Sheet name for `--query` mode |
| `--limit` | none | Row limit per table |
| `--verbose`, `-v` | | Verbose logging |

## Default Report Tables

When no `--tables` is specified, the report includes:

1. `daily_summary` -- Daily bot traffic aggregates
2. `url_performance` -- Per-URL metrics
3. `bot_requests_daily` -- Clean individual requests
4. `query_fanout_sessions` -- Detected query fan-out sessions

## Python API

```python
from llm_bot_pipeline.storage import get_backend
from llm_bot_pipeline.reporting import ExcelExporter

backend = get_backend("sqlite", db_path="data/llm-bot-logs.db")
backend.initialize()

exporter = ExcelExporter(backend)

# Full report
exporter.export_report("report.xlsx")

# Single table with filters
exporter.export_table(
    "daily_summary",
    "daily.xlsx",
    filters={"bot_category": "user_request"},
    limit=1000,
)

# Custom query
exporter.export_query(
    "SELECT bot_name, COUNT(*) as cnt FROM bot_requests_daily GROUP BY bot_name ORDER BY cnt DESC",
    "top-bots.xlsx",
    sheet_name="Top Bots",
)
```

## Features

- **Multi-sheet workbooks** -- each table gets its own sheet
- **Auto-column-widths** -- columns sized to fit content
- **Header styling** -- bold white text on blue background, frozen header row
- **Date formatting** -- dates and timestamps formatted for Excel
- **Works with any backend** -- SQLite and BigQuery both supported
