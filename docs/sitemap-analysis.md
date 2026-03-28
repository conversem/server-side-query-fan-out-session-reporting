# Sitemap Freshness & URL Decay Analysis

> **v2.1.2 Update:** This pipeline now supports multi-domain deployments. All sitemap tables include
> a `domain` column and all views expose `domain` as a filter dimension. See
> [migration-v2.1.1-to-v2.1.2.md](migration-v2.1.1-to-v2.1.2.md) for upgrade instructions.

## Multi-Domain Setup

When running with multiple domains, the `domain` column distinguishes sitemap entries and sessions
per website. All sitemap views (`v_url_freshness`, `v_url_freshness_detail`, `v_decay_*`) are
scoped by domain, so filtering by `domain` in Looker Studio gives correct per-domain numbers.

New views for multi-domain analysis:
- **`v_url_freshness_detail`** — per-URL freshness with `full_url` and `months_since_lastmod`. Filter by `url_path LIKE '/section%'` to analyze a content section.
- **`v_sessions_by_content_age`** — sessions joined to sitemap age. Filter `months_since_lastmod >= 6` to see LLM activity on stale content.
- **`v_url_performance_with_freshness`** — URL traffic with lastmod date. `WHERE lastmod IS NULL` finds pages cited by bots but absent from the sitemap.
- **`v_decay_unique_urls_by_domain`** / **`v_decay_request_volume_by_domain`** — per-domain decay curves with correct per-domain denominators.

## Overview

The sitemap analysis module cross-references your XML sitemaps with bot request data to answer two questions:

1. **Freshness** — Which sitemap URLs are being crawled by LLM bots, how often, and by how many distinct bots?
2. **Volume Decay** — How does crawl volume change over weekly/monthly periods for each URL?

This works with both SQLite and BigQuery backends.

## Configuration

Add sitemap URLs to your config:

```yaml
sitemaps:
  - "https://example.com/sitemap.xml"
  - "https://example.com/sitemap-blog.xml"
```

Or pass them directly via the API.

## Pipeline Stages

### 1. Fetch & Store

Parses XML sitemaps and stores entries in `sitemap_urls`:

```bash
# Integrated into the main pipeline
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07

# Or run sitemap stage standalone
python scripts/run_aggregations.py --backend sqlite
```

Each entry captures:

| Column | Description |
|--------|-------------|
| `url` | Full URL from sitemap |
| `url_path` | Path component (used to join with `bot_requests_daily`) |
| `lastmod` | Last modification date from sitemap XML |
| `lastmod_month` | Year-month derived from `lastmod` |
| `sitemap_source` | Which sitemap XML the URL came from |

### 2. Freshness Aggregation

Joins `sitemap_urls` with `bot_requests_daily` to populate `sitemap_freshness`:

| Column | Description |
|--------|-------------|
| `url_path` | Sitemap URL path |
| `first_seen_date` | Earliest request date for this URL |
| `last_seen_date` | Most recent request date |
| `request_count` | Total bot requests |
| `unique_urls` | Distinct full request URIs (includes query strings) |
| `unique_bots` | Distinct bot providers |
| `days_since_lastmod` | Days between `lastmod` and reference date |
| `lastmod_month` | Content publication month |

### 3. Volume Decay Aggregation

Computes per-URL request counts per time period in `url_volume_decay`:

| Column | Description |
|--------|-------------|
| `url_path` | Sitemap URL path |
| `period` | `week` or `month` |
| `period_start` | Start date of the period |
| `request_count` | Requests in this period |
| `unique_urls` | Distinct request URIs in this period |
| `unique_bots` | Distinct bot providers in this period |
| `prev_request_count` | Requests in the previous period |
| `decay_rate` | `(current - previous) / previous` |

## Tables

| Table | Purpose |
|-------|---------|
| `sitemap_urls` | Raw sitemap entries (populated from XML) |
| `sitemap_freshness` | Per-URL freshness metrics (re-computed each run) |
| `url_volume_decay` | Per-URL per-period volume with decay rates |

## Dashboard Queries

`SitemapAggregator` provides four helper methods for Looker Studio or custom dashboards:

| Method | Returns |
|--------|---------|
| `get_freshness_heatmap()` | URL freshness vs crawl frequency |
| `get_decay_curves(period)` | Request volume over time by content age cohort |
| `get_coverage_gaps()` | Sitemap URLs with zero bot requests |
| `get_freshness_summary()` | Per-month coverage percentage and averages |

## Excel Report Sheets

The export script (`scripts/export_session_report.py`) includes sitemap data in these sheets:

| Sheet | Source | Description |
|-------|--------|-------------|
| URL Freshness | `bot_requests_daily` + `sitemap_urls` | Pivot of session_date x lastmod_month |
| Decay Unique URLs | `bot_requests_daily` + `sitemap_urls` | Cumulative % of unique URLs within last N months |
| Decay Request Volume | `bot_requests_daily` + `sitemap_urls` | Cumulative % of request volume within last N months |
| Sitemap Summary | `sitemap_freshness` | Per-URL freshness metrics with unique_urls and unique_bots |
| URL Volume Decay | `url_volume_decay` | Per-URL per-period volume with decay rates |

## Programmatic Usage

```python
from llm_bot_pipeline.sitemap import run_sitemap_pipeline
from llm_bot_pipeline.storage import get_backend

backend = get_backend("sqlite", db_path="data/logs.db")
backend.initialize()

result = run_sitemap_pipeline(
    backend,
    sitemap_urls=["https://example.com/sitemap.xml"],
    lookback_days=365,
)
print(result)
# {'fetch': {'success': True, 'urls_stored': 150},
#  'aggregations': [SitemapAggregationResult(...), ...]}
```

Or use the aggregator directly:

```python
from llm_bot_pipeline.reporting.sitemap_aggregations import SitemapAggregator

agg = SitemapAggregator(backend)
freshness = agg.aggregate_freshness()
decay_weekly = agg.aggregate_volume_decay(period="weekly")
decay_monthly = agg.aggregate_volume_decay(period="monthly")
```
