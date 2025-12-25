# Server-Side Query Fan-Out Session monitoring & Reporting

A research framework for **server-side LLM activity tracking**, including our newly introduced **Query Fan-Out Session** tracking methodology.

📄 **Read the research article:** [The Query Fan-Out Session: Server-side Query Fan-Out Tracking](https://conversem.com/the-query-fan-out-session/)

## What is a Query Fan-Out Session?

A **Query Fan-Out Session** is a bundle of web requests from an LLM chat assistant that originated from a single user question. When LLM-powered services (like ChatGPT, Perplexity, or Claude) process user queries, they fan out multiple rapid requests to gather information—often 4-5 requests within 10-20ms from a single user question. By detecting these bursts, we can group requests into meaningful sessions that represent actual user interactions.

**Key research findings:**
- Most common gap between requests: **9ms**
- 84% of request gaps: **≤ 20ms**
- Optimal bundling window: **100ms** (91%+ sessions maintain high thematic coherence)

This framework allows you to reproduce the research and apply it to your own server logs:

1. **Ingests** Cloudflare logs via the Logpull API
2. **Identifies** request bundles using temporal and semantic analysis
3. **Optimizes** the time window for accurate session detection
4. **Reports** bundled sessions in CSV/Excel format

## Key Features

- **Research-backed methodology**: Uses OptScore composite metric for window optimization
- **Semantic analysis**: TF-IDF and Transformer-based URL embeddings
- **Session refinement**: Collision detection and semantic splitting for improved purity
- **Provider-specific tuning**: Different bots have different behaviors
- **Reproducible experiments**: Configurable parameters with validation

## Quick Start

### Prerequisites

**Required:**
- Python 3.11+
- pip or pipenv for package management
- 2GB+ free disk space for logs and database

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

pip install -r requirements.txt
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
# Edit config.yaml with your Cloudflare credentials
sops -e config.yaml > config.enc.yaml
rm config.yaml  # Remove unencrypted version
```

See [docs/sops/quickstart.md](docs/sops/quickstart.md) for detailed instructions.

### 3. Ingest Logs

**From Cloudflare API:**
```bash
# Pull last 7 days of logs from Cloudflare
python scripts/ingest_logs.py --provider cloudflare --input api://zone_id \
  --start-date 2024-01-01 --end-date 2024-01-07
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
python scripts/run_pipeline.py --start-date 2024-01-01 --end-date 2024-01-07
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
# Export to Excel
python scripts/export_session_report.py --format xlsx --output data/reports/sessions.xlsx

# Export to CSV with filters
python scripts/export_session_report.py \
    --start-date 2024-01-01 \
    --provider OpenAI \
    --output data/reports/openai_sessions.csv
```

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
│   ├── ingestion/       # Multi-provider log ingestion (8 adapters)
│   ├── cloudflare/      # Logpull API integration
│   ├── storage/         # SQLite storage layer
│   ├── pipeline/        # ETL processing
│   ├── research/        # Window optimization algorithms
│   └── reporting/       # Session aggregation & export
├── scripts/             # CLI entry points
├── docs/                # Documentation
└── tests/               # Test suite
```

See [docs/architecture.md](docs/architecture.md) for detailed architecture.

## Sample Data

Generate synthetic data for testing:

```bash
python scripts/generate_sample_data.py --output data/sample_requests.csv --count 5000
```

## Configuration

### config.example.yaml

```yaml
storage:
  backend: "sqlite"
  sqlite_db_path: "data/llm-bot-logs.db"

cloudflare:
  api_token: "your-cloudflare-api-token"
  zone_id: "your-zone-id"
```

## Security

The ingestion pipeline includes multiple security layers for processing untrusted log data:

- **Path Traversal Protection** - Prevents directory escape attacks with `--base-dir`
- **Input Sanitization** - Cleans field values and removes control characters
- **Field Length Limits** - Prevents DoS via oversized fields
- **Rate Limiting** - Protects API endpoints from abuse
- **File Size Limits** - Configurable with `--max-file-size`

See [docs/ingestion/security.md](docs/ingestion/security.md) for detailed security documentation.

## Documentation

- [Architecture Overview](docs/architecture.md)
- [Security Guide](docs/ingestion/security.md)
- [CLI Usage](docs/ingestion/cli-usage.md)
- [Provider Guides](docs/ingestion/providers/)
  - [AWS CloudFront](docs/ingestion/providers/aws-cloudfront.md)
  - [AWS ALB](docs/ingestion/providers/aws-alb-format.md)
  - [Cloudflare](docs/ingestion/providers/cloudflare.md)
  - [Azure CDN](docs/ingestion/providers/azure-cdn.md)
  - [Google Cloud CDN](docs/ingestion/providers/gcp-cdn-format.md)
  - [Fastly](docs/ingestion/providers/fastly-format.md)
  - [Akamai](docs/ingestion/providers/akamai-format.md)
- [SOPS Quick Start](docs/sops/quickstart.md)
- [Research Article: The Query Fan-Out Session](https://conversem.com/the-query-fan-out-session/)

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) before submitting PRs.

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0).

### What this means

- ✅ **Free to use** for internal tools, consulting, and client services
- ✅ **Free to modify** and adapt for your organization
- ✅ **Free to distribute** with attribution
- ⚠️ **SaaS/hosted services**: If you offer this as a hosted service, AGPL-3.0 requires you to release your source code under the same license.

### Commercial Licensing

For organizations that want to incorporate this into proprietary SaaS products without the AGPL-3.0 open-source requirements, commercial licenses are available. [Contact me](https://conversem.com/contact/).

## Citation

If you use this framework in your research, please cite:

> Remy, R. (2025). *Query Fan-Out Session Analysis: Determining Optimal Time Windows for LLM Bot Request Bundling*. Conversem Research Report.

```bibtex
@article{remy2025queryfanout,
  author = {Remy, Ruben},
  title = {The Query Fan-Out Session: Server-side Query Fan-Out Tracking},
  year = {2025},
  url = {https://conversem.com/the-query-fan-out-session/},
  publisher = {Conversem}
}
```

## About

This open-source release accompanies the research article on [The Query Fan-Out Session: Server-side Query Fan-Out Tracking](https://conversem.com/the-query-fan-out-session/). The methodology enables publishers to understand how their content contributes to answering real user questions in AI interfaces—moving beyond simple request counting to meaningful session-based metrics.
