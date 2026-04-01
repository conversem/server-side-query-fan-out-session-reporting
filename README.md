# Server-Side Query Fan-Out Session monitoring & Reporting

[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Release](https://img.shields.io/badge/release-v2.1.3-brightgreen.svg)](CHANGELOG.md)

A research framework and reporting pipeline for **server-side LLM activity tracking**, including our newly introduced
**Query Fan-Out Session** tracking methodology.

📄 **Read the research article:** [The Query Fan-Out Session: Server-side Query Fan-Out Tracking](https://conversem.com/the-query-fan-out-session/)

## What is a Query Fan-Out Session?

A **Query Fan-Out Session** is a bundle of web requests from an LLM chat assistant that
originated from a single user question. When LLM-powered services (like ChatGPT, Perplexity,
or Claude) process user queries, they fan out multiple rapid requests to gather information —
often 4–5 requests within 10–20ms from a single user question. By detecting these bursts,
we can group requests into meaningful sessions that represent actual user interactions.

**Key research findings:**

| Observation | Value |
|---|---|
| Most common gap between requests | **9ms** |
| Median gap between requests | **10ms** |
| 84% of all request gaps | **≤ 20ms** |
| 90% of all request gaps | **≤ 53ms** |
| Sessions with high thematic coherence at 100ms window | **91%+** |
| Over-bundling rate at 100ms window | **0.04%** |
| Validation ranking agreement on hold-out data | **100%** |

**Recommendation: 100ms as the standard bundling window.** Both OpenAI and Perplexity show
consistent burst patterns (17–22ms median). The 100ms threshold captures genuine query
fan-outs while virtually eliminating false merges between unrelated queries.

**Multi-page sessions reveal topical authority.** When an LLM pulls 2–4 pages from your
site in a single fan-out, the AI found multiple relevant pieces of content for one question.
The grouped URLs also reveal the decision journey the LLM is composing an answer for.

This framework allows you to reproduce the research and create query-fanout-session reporting applied to your own server logs:

1. **Ingest** CDN logs from 8+ providers (Cloudflare, AWS ALB, AWS CloudFront, Azure CDN,
   GCP CDN, Fastly, Akamai, Universal CSV/JSON/NDJSON) or via the Cloudflare Logpull API
2. **Identify** request bursts using temporal bundling with the 100ms window
3. **Refine** sessions with MIBCS collision detection — splits accidentally merged queries
   using graph-based semantic analysis
4. **Report** Query Fan-Out Sessions in multi-sheet Excel workbooks — sessions, URL-level
   performance, daily KPIs, sitemap freshness coverage, URL co-occurrence, and decay trends;
   or query directly via 11 pre-built SQLite analytical views

## Key Features of the Open Source Release

- **Research-backed methodology**: Uses OptScore composite metric for window optimization
- **Semantic analysis**: TF-IDF and Transformer-based URL embeddings for session coherence
- **Session refinement**: MIBCS collision detection and semantic splitting for improved purity
- **Provider-specific tuning**: Different bots have different burst behaviors
- **Reproducible experiments**: Configurable parameters with hold-out validation
- **8+ ingestion providers**: Modular plugin architecture — add new CDN providers in one class
- **Multi-domain support**: Per-domain SQLite databases with domain column throughout
- **Multi-sheet Excel reporting**: Full workbooks — sessions, URL performance, daily KPIs,
  sitemap freshness, URL co-occurrence, decay trends, and raw requests; configurable per export
- **Sitemap freshness tracking**: Fetch and parse XML sitemaps; track URL coverage by
  `lastmod` month, decay rates, and what percentage of your sitemap LLMs are actually requesting
- **11 pre-built SQLite analytical views**: From daily KPIs to URL co-occurrence and freshness decay
- **Local BigQuery modes**: `local_bq_buffered` (SQLite → BigQuery) and
  `local_bq_streaming` (in-memory → BigQuery) for cloud-ready local pipelines
- **Database migration tooling**: Idempotent v1→v2 schema migration with dry-run support

## Enterprise: Managed Cloud Pipeline

For organizations needing production-grade LLM traffic analysis at scale:

| Feature | Open Source | Enterprise |
|---------|:-----------:|:----------:|
| 8+ ingestion providers | ✓ | ✓ |
| SQLite local analysis | ✓ | ✓ |
| Multi-domain (SQLite per domain) | ✓ | ✓ |
| Session analysis (MIBCS) | ✓ | ✓ |
| Excel reporting | ✓ | ✓ |
| Local BigQuery modes | ✓ | ✓ |
| **GCP BigQuery cloud pipeline** | — | ✓ |
| **Cloud Run automated scheduling** | — | ✓ |
| **Looker Studio dashboards** | — | ✓ |
| **Multi-domain cloud orchestration** | — | ✓ |
| **Monitoring & alerting** | — | ✓ |
| **Dedicated support** | — | ✓ |

[Contact us](https://forms.gle/qoyLhf5K4p8399sG9) for enterprise pricing and implementation.

## Quick Start

### Prerequisites

**Required:**
- Python 3.10+
- pip for package management
- 2 GB+ free disk space for logs and database

**For Cloudflare Logpull API:**
- Cloudflare account with API access
- API token with "Zone Logs:Read" permission
- Zone ID for your domain

**For File-Based Ingestion:**
- Exported log files from your CDN provider
- Supported formats: CSV, JSON, NDJSON, W3C Extended Log Format
- Gzip compression supported (.gz files)

**For Secrets Management:**
- SOPS and Age for encrypted configuration

### 1. Clone and Setup

```bash
git clone https://github.com/conversem/server-side-query-fan-out-session-reporting.git
cd server-side-query-fan-out-session-reporting

python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: .\venv\Scripts\Activate.ps1  # Windows PowerShell

pip install .
# For session refinement (ML): pip install ".[ml]"
# For research visualizations: pip install ".[viz]"
```

### 2. Configure Secrets (Required)

This project uses SOPS for secure secret management.

```bash
# Install SOPS and Age
brew install sops age  # macOS
# See docs/sops/quickstart.md for Linux/Windows

# Generate your encryption key
mkdir -p ~/.sops/age
age-keygen -o ~/.sops/age/keys.txt
# Note the public key from the output

# Configure SOPS
cp .sops.yaml.example .sops.yaml
# Edit .sops.yaml and replace age1YOUR_PUBLIC_KEY_HERE with your key

# Set environment variable (add to ~/.bashrc or ~/.zshrc)
export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt

# Create and encrypt your config
cp config.example.yaml config.yaml
# Edit config.yaml with your credentials
sops -e config.yaml > config.enc.yaml
rm config.yaml  # Remove unencrypted version
```

See [docs/sops/quickstart.md](docs/sops/quickstart.md) for detailed instructions.

### 3. Ingest Logs

**From Cloudflare Logpull API:**
```bash
python scripts/ingest_logs.py --provider cloudflare --input api://zone_id \
  --start-date 2025-01-01 --end-date 2025-01-07
```

**From Exported Files (8 providers supported):**
```bash
# AWS CloudFront (W3C format)
python scripts/ingest_logs.py --provider aws_cloudfront --input ./cloudfront-logs/

# AWS ALB access logs
python scripts/ingest_logs.py --provider aws_alb --input ./alb-logs/

# Cloudflare (JSON/CSV)
python scripts/ingest_logs.py --provider cloudflare --input ./cloudflare-export.json

# Azure CDN / Front Door
python scripts/ingest_logs.py --provider azure_cdn --input ./azure-logs.json

# Google Cloud CDN
python scripts/ingest_logs.py --provider gcp_cdn --input ./gcp-logs.json

# Fastly
python scripts/ingest_logs.py --provider fastly --input ./fastly-logs.json

# Akamai DataStream
python scripts/ingest_logs.py --provider akamai --input ./akamai-logs.json

# Universal format (CSV/JSON from any provider)
python scripts/ingest_logs.py --provider universal --input ./logs.csv
```

See [Provider Guides](docs/ingestion/providers/) for detailed export instructions.

### 4. Run ETL Pipeline

```bash
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07
```

### 5. Run Window Optimization Experiment

```bash
# Run with default settings
python scripts/run_window_experiment.py

# Custom windows
python scripts/run_window_experiment.py --windows 50,100,500,1000,3000
```

### 6. Export Session Reports

```bash
# Export to Excel (multi-sheet: sessions, URL performance, KPIs, sitemap freshness)
python scripts/export_session_report.py --output data/reports/sessions.xlsx

# Export to CSV with filters
python scripts/export_session_report.py \
    --start-date 2025-01-01 \
    --provider OpenAI \
    --output data/reports/openai_sessions.csv
```

## Ingestion Providers

| Provider | Format | Configuration |
|----------|--------|---------------|
| **Cloudflare** | Logpull API / Logpush files | `--provider cloudflare` |
| **AWS ALB** | Access log format | `--provider aws_alb` |
| **AWS CloudFront** | W3C extended log format | `--provider aws_cloudfront` |
| **Azure CDN** | Access log format | `--provider azure_cdn` |
| **GCP Cloud CDN** | HTTP/S load balancer logs | `--provider gcp_cdn` |
| **Fastly** | Access log format | `--provider fastly` |
| **Akamai** | Datastream 2 format | `--provider akamai` |
| **Universal** | CSV / JSON / NDJSON | `--provider universal` |

See [docs/ingestion/](docs/ingestion/README.md) for provider-specific guides.

## Pipeline Modes

| Mode | Storage | Compute | Requirements |
|------|---------|---------|-------------|
| `local_sqlite` | SQLite | Local Python | None (default) |
| `local_bq_buffered` | SQLite → BigQuery | Local Python | GCP project |
| `local_bq_streaming` | Memory → BigQuery | Local Python | GCP project |

```bash
export PROCESSING_MODE=local_sqlite   # default
```

## Multi-Domain Analysis

Run independent analysis for multiple websites with isolated databases:

```bash
python scripts/run_multi_domain.py \
  --config config.enc.yaml \
  --start-date 2025-01-01 \
  --end-date 2025-01-07
```

Each domain gets its own SQLite database (`data/{domain}.db`). Domain is tracked as a
column in every table for cross-domain querying.

## Research Methodology

### OptScore Formula

The framework uses a composite optimization score:

```
OptScore = α·MIBCS + β·Silhouette + γ·BPS - δ·SingletonRate - ε·GiantRate - ζ·ThematicVariance
```

| Component | Weight | Description |
|-----------|--------|-------------|
| MIBCS | α=0.30 | Mean Intra-Bundle Cosine Similarity |
| Silhouette | β=0.25 | Cluster separation quality |
| BPS | γ=0.25 | Bundle Purity Score |
| SingletonRate | δ=0.10 | Penalty for single-request bundles |
| GiantRate | ε=0.05 | Penalty for oversized bundles |
| ThematicVariance | ζ=0.05 | Penalty for thematic inconsistency |

### Experiment Output

Running `run_window_experiment.py` produces:

- **Optimal window recommendation** with confidence level
- **Per-provider analysis** showing behavioral differences
- **Validation metrics** from hold-out testing
- **Visualization** of window comparisons

## Project Structure

```
├── src/llm_bot_pipeline/
│   ├── ingestion/       # Multi-provider log ingestion (8+ adapters, plugin registry)
│   ├── cloudflare/      # Logpull API integration
│   ├── storage/         # SQLite storage layer with migration support
│   ├── pipeline/        # ETL processing (local SQLite and BigQuery modes)
│   ├── research/        # Window optimization, MIBCS, semantic embeddings
│   ├── reporting/       # Session aggregation, Excel export, sitemap freshness
│   ├── sitemap/         # Sitemap parsing and URL coverage tracking
│   ├── schemas/         # Shared data schemas (CDN-agnostic)
│   └── monitoring/      # Retry handling and data quality checks
├── scripts/             # CLI entry points
│   └── migrations/      # Database schema migration scripts
├── docs/                # Documentation
└── tests/               # Test suite (unit + integration)
```

See [docs/architecture.md](docs/architecture.md) for detailed architecture.

## Sample Data

Generate synthetic data for testing:

```bash
python scripts/generate_sample_data.py --output data/sample_requests.csv --count 5000
```

## Upgrading from v1

If you have an existing v1 database:

```bash
# Dry run first
python scripts/migrations/migrate_v1_to_v2.py --db-path data/llm-bot-logs.db --dry-run

# Apply migration
python scripts/migrations/migrate_v1_to_v2.py --db-path data/llm-bot-logs.db
```

See [docs/migration-v1-to-v2.md](docs/migration-v1-to-v2.md) for the full upgrade guide.

## Configuration

### config.example.yaml

```yaml
storage:
  backend: "sqlite"           # or "bigquery" for local BQ modes
  sqlite_db_path: "data/llm-bot-logs.db"

cloudflare:
  api_token: "your-cloudflare-api-token"
  zone_id: "your-zone-id"
```

## Security

The ingestion pipeline includes multiple security layers for processing untrusted log data:

- **Path Traversal Protection** — Prevents directory escape attacks with `--base-dir`
- **Input Sanitization** — Cleans field values and removes control characters
- **Field Length Limits** — Prevents DoS via oversized fields
- **File Size Limits** — Configurable with `--max-file-size`

See [docs/ingestion/security.md](docs/ingestion/security.md) for detailed security documentation.

## Documentation

- [Architecture Overview](docs/architecture.md)
- [Processing Modes](docs/processing-modes.md)
- [Storage Backend Guide](docs/backend-guide.md)
- [Security Guide](docs/ingestion/security.md)
- [CLI Usage](docs/ingestion/cli-usage.md)
- [Provider Guides](docs/ingestion/providers/)
  - [AWS CloudFront](docs/ingestion/providers/aws-cloudfront.md)
  - [AWS ALB](docs/ingestion/providers/aws-alb-format.md)
  - [Cloudflare](docs/ingestion/providers/cloudflare.md)
  - [Fastly](docs/ingestion/providers/fastly-format.md)
  - [Akamai](docs/ingestion/providers/akamai-format.md)
  - [GCP Cloud CDN](docs/ingestion/providers/gcp-cdn-format.md)
- [SOPS Quick Start](docs/sops/quickstart.md)
- [Session Methodology](docs/query-fanout-sessions.md)
- [Excel Reporting](docs/reporting-excel.md)
- [Sitemap Analysis](docs/sitemap-analysis.md)
- [v1→v2 Migration Guide](docs/migration-v1-to-v2.md)
- [Research Article: The Query Fan-Out Session](https://conversem.com/the-query-fan-out-session/)

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) before
submitting PRs.

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0).

### What this means

- ✅ **Free to use** for internal tools, consulting, and client services
- ✅ **Free to modify** and adapt for your organization
- ✅ **Free to distribute** with attribution
- ⚠️ **SaaS/hosted services**: If you offer this as a hosted service, AGPL-3.0 requires
  you to release your source code under the same license.

### Commercial Licensing

For organizations that want to incorporate this into proprietary SaaS products without
the AGPL-3.0 open-source requirements, commercial licenses are available.
[Contact me](https://conversem.com/contact/).

## Citation

If you use this framework in your research, please cite:

> Remy, R. (2025). *Query Fan-Out Session Analysis: Determining Optimal Time Windows for
> LLM Bot Request Bundling*. Conversem Research Report.

```bibtex
@article{remy2025queryfanout,
  author    = {Remy, Ruben},
  title     = {The Query Fan-Out Session: Server-side Query Fan-Out Tracking},
  year      = {2025},
  url       = {https://conversem.com/the-query-fan-out-session/},
  publisher = {Conversem}
}
```

## About

This open-source release accompanies the research article on
[The Query Fan-Out Session: Server-side Query Fan-Out Tracking](https://conversem.com/the-query-fan-out-session/).
The methodology enables publishers to understand how their content contributes to answering
real user questions in AI interfaces — moving beyond simple request counting to meaningful
session-based metrics.

Version 2.0 expands the framework from a Cloudflare-only research prototype to a
production-ready pipeline supporting 8+ CDN providers, modular plugin architecture, and
multi-domain SQLite analysis.

For organizations requiring a fully managed cloud deployment (GCP BigQuery, Cloud Run,
Looker Studio dashboards), [enterprise support is available](https://conversem.com/contact/).
