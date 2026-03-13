# Excel Report Export

## Overview

`scripts/export_session_report.py` exports a multi-sheet Excel workbook (or flat CSV) containing session analysis, dashboard views, and sitemap freshness data. It works with both SQLite and BigQuery backends.

## Usage

```bash
# SQLite (default)
python scripts/export_session_report.py \
  --output data/reports/sessions.xlsx \
  --start-date 2025-01-01 --end-date 2025-01-31

# BigQuery
python scripts/export_session_report.py \
  --backend bigquery \
  --output data/reports/sessions.xlsx

# CSV (single sheet: Sessions only)
python scripts/export_session_report.py \
  --output data/reports/sessions.csv

# Filter by provider and confidence
python scripts/export_session_report.py \
  --output report.xlsx \
  --provider OpenAI \
  --min-confidence high
```

## CLI Options

| Flag | Description |
|------|-------------|
| `--output`, `-o` | Output file path (required) |
| `--format`, `-f` | `csv` or `xlsx` (auto-detected from extension) |
| `--backend` | `sqlite` or `bigquery` (default: from settings) |
| `--db-path` | SQLite database path (default: from settings) |
| `--start-date` | Start date filter (YYYY-MM-DD) |
| `--end-date` | End date filter (YYYY-MM-DD) |
| `--provider` | Filter by bot provider (e.g., OpenAI, Perplexity) |
| `--min-confidence` | Minimum confidence level: `high`, `medium`, or `low` |
| `--verbose`, `-v` | Enable debug logging |

## Excel Sheets

### Core Analysis Sheets

| Sheet | Description |
|-------|-------------|
| Sessions | All query fan-out sessions with confidence scores, URL counts, coherence metrics |
| Summary | Aggregated statistics (total sessions, avg URLs, confidence distribution) |
| Top URLs | Most frequently accessed URLs across all sessions |
| Provider Stats | Per-bot-provider session counts and metrics |
| URL Details | One row per URL per session (session_id, url, position) |

### Dashboard View Sheets

These are populated from pre-defined SQLite views (created during `backend.initialize()`). On BigQuery, each is queried individually and skipped if the view does not exist.

| Sheet | View | Description |
|-------|------|-------------|
| Daily KPIs | `v_daily_kpis` | Daily session count, avg URLs, singleton rate |
| URL Distribution | `v_session_url_distribution` | URL count buckets per day |
| Singleton Binary | `v_session_singleton_binary` | Single-URL vs multi-URL session split |
| Bot Volume | `v_bot_volume` | Session counts by bot provider |
| Top Topics | `v_top_session_topics` | Top session topics with coherence metrics |
| Category Comparison | `v_category_comparison` | User questions vs training data breakdown |
| URL Cooccurrence | `v_url_cooccurrence` | URL co-occurrence in multi-URL sessions |

### Sitemap & Freshness Sheets

| Sheet | Description |
|-------|-------------|
| URL Freshness | Pivot of session_date x lastmod_month (request count, sitemap URLs, % requested) |
| Decay Unique URLs | Cumulative % of unique URLs within last N months of content age |
| Decay Request Volume | Cumulative % of request volume within last N months of content age |
| Sitemap Summary | Per-URL freshness metrics from `sitemap_freshness` table |
| URL Volume Decay | Per-URL per-period volume with decay rates from `url_volume_decay` table |

See [Sitemap Analysis](sitemap-analysis.md) for details on the freshness/decay tables.

## Backend-Agnostic Design

The export script uses `backend.query()` for all data retrieval and `sql_compat.json_array_unnest()` for JSON array expansion (Top URLs sheet), making it fully compatible with both SQLite and BigQuery. The `--backend` flag (or settings) determines which backend is used at runtime.

## Programmatic Usage

```python
from scripts.export_session_report import generate_session_report

generate_session_report(
    backend=backend,
    output_path=Path("report.xlsx"),
    output_format="xlsx",
    start_date=date(2025, 1, 1),
    end_date=date(2025, 1, 31),
)
```
