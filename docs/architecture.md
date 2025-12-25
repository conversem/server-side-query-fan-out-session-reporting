# Project Architecture

## Overview

This project provides a framework for analyzing LLM bot traffic patterns and bundling requests into semantic query fan-out sessions. It supports ingestion from multiple CDN/cloud providers (AWS CloudFront, AWS ALB, Azure CDN, GCP CDN, Cloudflare, Fastly, Akamai) into a local SQLite database, then applies research-backed algorithms to identify session patterns.

## Folder Structure

```
server-side-query-fan-out-session-reporting/
├── src/                              # Main source code
│   └── llm_bot_pipeline/             # Python package
│       │
│       ├── config/                   # Configuration management
│       │   ├── settings.py           # Settings + SessionRefinementSettings
│       │   ├── constants.py          # Bot classifications, field names
│       │   └── sops_loader.py        # SOPS encrypted config loader
│       │
│       ├── cloudflare/               # Cloudflare Logpull integration
│       │   ├── logpull.py            # Logpull API client
│       │   └── filters.py            # LLM bot filter definitions
│       │
│       ├── ingestion/                # Multi-provider log ingestion
│       │   ├── base.py               # IngestionAdapter base class
│       │   ├── registry.py           # Provider registry
│       │   ├── file_utils.py         # Gzip auto-detection
│       │   ├── security.py           # Path traversal protection
│       │   ├── validation.py         # Schema validation
│       │   ├── parsers/              # Format-specific parsers
│       │   │   ├── csv_parser.py
│       │   │   ├── json_parser.py
│       │   │   └── w3c_parser.py
│       │   └── providers/            # Provider adapters
│       │       ├── universal/        # CSV/JSON/NDJSON
│       │       ├── aws_cloudfront/   # AWS CloudFront (W3C)
│       │       ├── aws_alb/          # AWS ALB
│       │       ├── azure_cdn/        # Azure CDN/Front Door
│       │       ├── gcp_cdn/          # Google Cloud CDN
│       │       ├── cloudflare/       # Cloudflare
│       │       ├── fastly/           # Fastly
│       │       └── akamai/           # Akamai DataStream
│       │
│       ├── storage/                  # SQLite storage layer
│       │   ├── base.py               # StorageBackend abstract class
│       │   ├── factory.py            # Backend factory pattern
│       │   └── sqlite_backend.py     # SQLite implementation
│       │
│       ├── pipeline/                 # ETL pipeline
│       │   ├── local_pipeline.py     # SQLite ETL pipeline
│       │   ├── sql_compat.py         # SQL compatibility layer
│       │   ├── extract.py            # Data extraction
│       │   └── transform.py          # Data transformation
│       │
│       ├── reporting/                # Analytics & reporting
│       │   ├── session_aggregations.py  # Session bundling logic
│       │   ├── local_aggregations.py    # Aggregation queries
│       │   └── local_dashboard_queries.py # Dashboard queries
│       │
│       ├── research/                 # Research methodology (core)
│       │   ├── experiment_runner.py  # Window optimization experiments
│       │   ├── temporal_analysis.py  # Time-based pattern analysis
│       │   ├── window_optimizer.py   # Optimal window calculation
│       │   └── semantic_embeddings.py # URL semantic similarity
│       │
│       ├── schemas/                  # Data schemas
│       │   ├── raw.py                # Raw table schema
│       │   ├── clean.py              # Clean/processed schema
│       │   └── bundles.py            # Session bundle schema
│       │
│       ├── monitoring/               # Operational utilities
│       │   └── retry_handler.py      # Retry logic with backoff
│       │
│       └── utils/                    # Shared utilities
│           ├── bot_classifier.py     # User-agent classification
│           ├── url_utils.py          # URL parsing utilities
│           └── http_utils.py         # HTTP status helpers
│
├── scripts/                          # CLI entry points
│   ├── ingest_logs.py                # Multi-provider log ingestion
│   ├── run_pipeline.py               # ETL pipeline execution
│   ├── run_window_experiment.py      # Research: optimal window
│   ├── run_aggregations.py           # Run aggregation queries
│   ├── run_dashboard_queries.py      # Dashboard query execution
│   ├── run_quality_checks.py         # Data quality validation
│   ├── export_session_report.py      # Export to CSV/Excel
│   ├── analyze_temporal_patterns.py  # Temporal pattern analysis
│   ├── plot_window_comparison.py     # Visualization generation
│   ├── backfill_sessions.py          # Session backfill
│   ├── generate_sample_data.py       # Generate test data
│   └── migrations/                   # Database migrations
│
├── tests/                            # Test suite
│   ├── unit/                         # Unit tests
│   ├── integration/                  # Integration tests
│   └── performance/                  # Performance benchmarks
│
├── docs/                             # Documentation
│   ├── ingestion/                    # Multi-provider ingestion docs
│   │   ├── cli-usage.md
│   │   ├── providers/                # Provider-specific guides
│   │   └── troubleshooting.md
│   ├── sops/                         # SOPS encryption guide
│   ├── testing/                      # Test documentation
│   └── architecture.md               # This file
│
├── data/                             # Local data storage (gitignored)
├── credentials/                      # Credentials (gitignored)
│
├── Dockerfile
├── docker-compose.yml
├── config.example.yaml               # Configuration template
├── requirements.txt
├── pyproject.toml
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Multi-Provider Log Sources                       │
├───────────┬───────────┬───────────┬───────────┬───────────┬─────────┤
│ Cloudflare│ AWS ALB   │ CloudFront│ Azure CDN │ Fastly    │ Akamai  │
└─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┴────┬────┘
      │           │           │           │           │          │
      └───────────┴───────────┴─────┬─────┴───────────┴──────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │  Ingestion Adapters  │
                         │  (ingestion/)        │
                         │  - Auto-detection    │
                         │  - Gzip support      │
                         │  - Schema validation │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │   SQLite Database    │
                         │   raw_bot_requests   │
                         └──────────┬───────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
     ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
     │    Pipeline     │  │    Research     │  │    Reporting    │
     │  (pipeline/)    │  │  (research/)    │  │  (reporting/)   │
     │  ETL Transform  │  │  Experiment     │  │  Session Export │
     └────────┬────────┘  │  Runner         │  │  CSV / Excel    │
              │           └────────┬────────┘  └─────────────────┘
              ▼                    │
     ┌─────────────────┐           │
     │ bot_requests_   │◄──────────┘
     │ daily           │  (reads from SQLite)
     └─────────────────┘
```

## Research Methodology

The core research component determines optimal time windows for bundling LLM bot requests into query fan-out sessions.

### Key Concepts

1. **Query Fan-Out Session**: A bundle of related requests from the same bot provider within a time window
2. **Time Window**: The maximum gap (in milliseconds) between requests in the same session
3. **OptScore**: A composite metric balancing multiple optimization objectives
4. **Session Refinement**: Post-bundling collision detection and semantic splitting for improved purity

### OptScore Formula

```
OptScore = α·MIBCS + β·Silhouette + γ·BPS - δ·SingletonRate - ε·GiantRate - ζ·ThematicVariance
```

Where:
- **MIBCS** (α=0.30): Mean Intra-Bundle Cosine Similarity
- **Silhouette** (β=0.25): Cluster separation quality
- **BPS** (γ=0.25): Bundle Purity Score
- **SingletonRate** (δ=0.10): Penalty for single-request bundles
- **GiantRate** (ε=0.05): Penalty for oversized bundles
- **ThematicVariance** (ζ=0.05): Penalty for thematic inconsistency

### Session Refinement (Collision Detection)

After initial temporal bundling, **session refinement** detects and splits collision bundles—cases where multiple independent queries were accidentally merged due to temporal proximity.

```
┌─────────────────────────────────────────────────────────────────────┐
│                       Session Refinement Flow                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. Initial Temporal Bundling                                        │
│     └─► Group requests by (provider, time_window)                   │
│                                                                      │
│  2. Collision Detection (if enabled)                                 │
│     └─► Flag bundles with low MIBCS (semantic incoherence)          │
│     └─► IP-based detection OFF by default (research: r=0.023)       │
│                                                                      │
│  3. Semantic Splitting                                               │
│     └─► Build URL similarity graph                                  │
│     └─► Find connected components (sub-bundles)                     │
│     └─► Accept split if MIBCS improves by threshold                 │
│                                                                      │
│  4. Output: Refined Sessions                                         │
│     └─► Higher purity bundles                                       │
│     └─► Better semantic coherence                                   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Configuration** (`config.example.yaml`):

```yaml
session_refinement:
  enabled: true                      # Master flag (ON by default)
  enable_semantic_refinement: true   # MIBCS-based splitting (recommended)
  enable_ip_based_refinement: false  # Research shows IP doesn't help (r=0.023)
  similarity_threshold: 0.5          # Min URL similarity to create edge
  min_sub_bundle_size: 2             # Min requests per sub-bundle
  min_mibcs_improvement: 0.05        # Min improvement to accept split
```

**Research Finding**: IP diversity does not discriminate between clean bundles and collisions (correlation r=0.023). See [IP Fingerprint Analysis](https://conversem.com/ip-addresses-dont-help-detect-query-fan-out-sessions/) for details.

### Experiment Pipeline

The experiment runner reads directly from SQLite database:

```bash
# Run with default database
python scripts/run_window_experiment.py

# Custom database path
python scripts/run_window_experiment.py --db-path data/my-logs.db

# Custom options
python scripts/run_window_experiment.py \
    --db-path data/llm-bot-logs.db \
    --table-name bot_requests_daily \
    --windows 100,500,1000,3000,5000 \
    --embedding-method tfidf
```

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Window Optimization Experiment                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. Load Data from SQLite (bot_requests_daily table)                 │
│     └─► Filter by bot category (user_request)                       │
│     └─► Exclude noise providers (Microsoft/Bing)                    │
│                                                                      │
│  2. Generate Candidate Windows                                       │
│     └─► [100ms, 500ms, 1000ms, 3000ms, 5000ms]                      │
│                                                                      │
│  3. For Each Window:                                                 │
│     └─► Bundle requests by (provider, time_window)                  │
│     └─► Calculate semantic embeddings (TF-IDF or Transformer)       │
│     └─► Compute OptScore metrics                                    │
│                                                                      │
│  4. Select Optimal Window                                            │
│     └─► Highest OptScore                                            │
│     └─► Cross-validation agreement                                  │
│     └─► Confidence level (high/medium/low)                          │
│                                                                      │
│  5. Generate Report                                                  │
│     └─► Recommendation with confidence                              │
│     └─► Per-provider analysis                                       │
│     └─► Visualization outputs                                       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Storage Layer

The project uses SQLite for local data storage:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SQLite Database                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   Tables:                                                            │
│   ├── raw_bot_requests      # Ingested Cloudflare logs              │
│   ├── bot_requests_daily    # Cleaned/enriched daily data           │
│   ├── query_fanout_sessions # Bundled sessions                      │
│   └── daily_summary         # Aggregated metrics                    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Configuration

### Secrets Management

All sensitive configuration is encrypted using SOPS:

```
.sops.yaml.example  →  Copy  →  .sops.yaml (with your key)
                                      │
config.example.yaml  →  Encrypt  →  config.enc.yaml
                                      │
                                sops_loader.py
                                      │
                                      ▼
                               Settings object
```

See [docs/sops/quickstart.md](sops/quickstart.md) for setup instructions.

### Environment Variables (Alternative)

| Variable | Description |
|----------|-------------|
| `SQLITE_DB_PATH` | SQLite database path (default: `data/llm-bot-logs.db`) |
| `CLOUDFLARE_API_TOKEN` | Cloudflare API token |
| `CLOUDFLARE_ZONE_ID` | Cloudflare zone ID |
| `SESSION_REFINEMENT_ENABLED` | Enable session refinement (default: `true`) |
| `SESSION_REFINEMENT_SEMANTIC` | Enable semantic splitting (default: `true`) |
| `SESSION_REFINEMENT_SIMILARITY_THRESHOLD` | URL similarity threshold (default: `0.5`) |

## Design Principles

### Modularity
- Each module has a single responsibility
- Small, focused files (< 200 lines ideally)
- Clear interfaces between modules

### Research-First
- Core algorithms are isolated in `research/` module
- Configurable parameters for experimentation
- Reproducible results with seed control

### LLM/AI Friendly
- Small context per file for efficient AI assistance
- Clear naming conventions
- Self-documenting code structure

### Human Friendly
- Intuitive navigation
- Logical grouping by domain
- Comprehensive documentation
