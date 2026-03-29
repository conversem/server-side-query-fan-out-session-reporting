# Storage Backend Guide

## Choosing a Backend

| Criterion | SQLite | BigQuery (local modes) |
|-----------|--------|----------------------|
| **Best for** | Local dev, single-machine analysis | Local pipeline pushing to cloud |
| **Setup** | Zero config (auto-creates file) | GCP project + service account |
| **Reporting** | Multi-sheet Excel workbooks | Looker Studio (Enterprise) |
| **Cost** | Free | Pay-per-query (GCP billing) |
| **Concurrency** | Single-writer | Unlimited concurrent readers |
| **Streaming** | No | Yes (BigQuery Storage API) |
| **Partitioning** | No | Date-partitioned + clustered |
| **Max data volume** | ~10 GB practical limit | Petabyte-scale |

**Decision tree:**

- Analyzing a single site for a report? → **SQLite**
- Want multi-sheet Excel output with zero cloud? → **SQLite**
- Running multi-domain analysis locally? → **SQLite** (per-domain databases)
- Want to push data to BigQuery from your machine? → **local_bq_buffered** or **local_bq_streaming**
- Need fully-managed production pipeline with dashboards? → **Enterprise** (see below)

## SQLite Backend

### Configuration

SQLite is the default backend. Minimal configuration required:

```yaml
# config.example.yaml
storage:
  backend: "sqlite"
  sqlite_db_path: "data/llm-bot-logs.db"
```

Or via environment variables:

```bash
export STORAGE_BACKEND=sqlite
export SQLITE_DB_PATH=data/llm-bot-logs.db
```

### Database Files

The pipeline supports two database modes:

| Mode | Database Path | Entry Point |
|------|--------------|-------------|
| **Single-domain** | `data/llm-bot-logs.db` (default) | `run_pipeline.py` |
| **Multi-domain** | `data/{domain}.db` per domain | `run_multi_domain.py` |

Multi-domain mode creates a separate database per domain, configured in `config.enc.yaml`:

```yaml
domains:
  - domain: "example.com"
    zone_id: "your-zone-id"
    db_name: "example.db"        # → data/example.db
    sitemaps:
      - "https://example.com/sitemap.xml"
  - domain: "example.org"
    zone_id: "your-other-zone-id"
    db_name: "example-org.db"    # → data/example-org.db
```

### Schema

```
Tables:
├── raw_bot_requests         Ingested raw log records
├── bot_requests_daily       Cleaned/enriched daily data
├── query_fanout_sessions    Bundled sessions (natural key dedup)
├── session_url_details      One row per URL per session
├── session_refinement_log   Refinement audit trail
├── daily_summary            Aggregated daily metrics
├── url_performance          Per-URL performance metrics
├── data_freshness           Data freshness tracking
├── sitemap_urls             Sitemap URL entries
├── sitemap_freshness        Per-URL freshness metrics
└── url_volume_decay         Per-URL volume with decay rates

Views:
├── v_daily_kpis             Daily key performance indicators
├── v_session_url_distribution  URL count buckets per day
├── v_session_singleton_binary  Singleton vs multi-URL split
├── v_bot_volume             Session counts by bot provider
├── v_top_session_topics     Top session topics with metrics
├── v_category_comparison    User questions vs training data
├── v_url_cooccurrence       URL co-occurrence in multi-URL sessions
├── v_url_freshness          URL freshness status
└── v_decay_request_volume   Request volume over decay periods
```

### Typical Workflow

```bash
# 1. Ingest logs
python scripts/ingest_logs.py --provider cloudflare --input ./logs/ \
  --start-date 2025-01-01 --end-date 2025-01-07

# 2. Run ETL pipeline
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07

# 3. Run aggregations (optional refresh)
python scripts/run_aggregations.py --start-date 2025-01-01 --end-date 2025-01-07

# 4. Export Excel report
python scripts/export_session_report.py --output data/reports/sessions.xlsx
```

### Database Initialization and Migration

The `SQLiteBackend.initialize()` method creates all tables, views, and indexes if they
don't exist. It also auto-migrates v1 databases by adding missing columns via
`ALTER TABLE`.

**Upgrading from v1:** See [migration-v1-to-v2.md](migration-v1-to-v2.md) for the full upgrade guide.

## Local BigQuery Modes

For users who want to store data in BigQuery while running the pipeline locally:

```bash
pip install ".[gcp]"
```

### Setup

```yaml
# config.enc.yaml
storage:
  backend: "sqlite"              # for raw data buffering

gcp:
  project_id: "your-project-id"
  location: "EU"
  dataset_id: "bot_logs"
```

### local_bq_buffered

Raw data buffered in SQLite, clean output pushed to BigQuery:

```bash
export PROCESSING_MODE=local_bq_buffered
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07
```

### local_bq_streaming

No SQLite; transforms run in memory and stream directly to BigQuery:

```bash
export PROCESSING_MODE=local_bq_streaming
python scripts/ingest_logs.py --provider cloudflare --input ./logs/ --stream
```

See [processing-modes.md](processing-modes.md) for a full comparison of modes.

## Enterprise: Fully-Managed BigQuery Pipeline

For production workloads requiring automated scheduling, Looker Studio dashboards,
monitoring, and multi-domain cloud orchestration, we offer an enterprise managed
implementation on your own GCP infrastructure.

[Contact us](https://conversem.com/contact/) for enterprise details.
