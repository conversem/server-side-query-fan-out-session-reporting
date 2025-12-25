# Multi-Provider Log Ingestion

## Overview

The ingestion system provides a unified interface for importing log data from multiple CDN and cloud providers into a standardized format. It supports ingestion from:

- **Universal formats** (CSV, JSON, NDJSON) - Works with any provider
- **AWS CloudFront** - W3C extended log format from S3 exports
- **Cloudflare** - Logpull API or Logpush file exports

All ingested logs are normalized to a **universal schema** that enables consistent analysis across providers.

## Quick Start

### Option 1: Universal CSV/JSON Format

If your logs are already in CSV or JSON format with standard field names:

```bash
# CSV format
python scripts/ingest_logs.py --provider universal --input logs.csv

# JSON Lines format
python scripts/ingest_logs.py --provider universal --input logs.ndjson

# Directory of files
python scripts/ingest_logs.py --provider universal --input logs/
```

### Option 2: AWS CloudFront

For CloudFront logs exported from S3:

```bash
# Single file
python scripts/ingest_logs.py --provider aws_cloudfront --input cloudfront-logs/2024-01-15.log

# Directory of logs
python scripts/ingest_logs.py --provider aws_cloudfront --input cloudfront-logs/
```

### Option 3: Cloudflare

**Via Logpull API** (requires configuration):

```bash
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input api://zone_id \
  --start-date 2024-01-15 \
  --end-date 2024-01-16
```

**Via Logpush files**:

```bash
# CSV file
python scripts/ingest_logs.py --provider cloudflare --input logpush.csv

# JSON file
python scripts/ingest_logs.py --provider cloudflare --input logpush.json
```

### Auto-Detection

The system can automatically detect the provider from file format:

```bash
# Auto-detect provider from file
python scripts/ingest_logs.py --input logs.csv
```

### List Available Providers

```bash
python scripts/ingest_logs.py --list-providers
```

### Validate Without Ingesting

Test your files before ingestion:

```bash
python scripts/ingest_logs.py --provider universal --input logs.csv --validate-only
```

## Features

- **Multi-format support**: CSV, TSV, JSON, NDJSON, W3C extended logs
- **Automatic compression handling**: Transparently handles gzip-compressed files
- **Bot filtering**: Filter to only LLM bot traffic (GPTBot, ChatGPT-User, ClaudeBot, etc.) - **enabled by default**
- **Time-based filtering**: Ingest only records within a specific time range
- **Directory processing**: Recursively process directories of log files
- **Validation**: Comprehensive validation with detailed error reports
- **Progress reporting**: Real-time progress updates for large files

## Documentation Structure

- **[Universal Format Specification](universal-format.md)** - Field definitions and format requirements
- **[Provider Guides](providers/)** - Provider-specific setup and export instructions
  - [AWS CloudFront](providers/aws-cloudfront.md)
  - [Cloudflare](providers/cloudflare.md)
  - [Other Providers](providers/other-providers.md) - Azure, GCP, Fastly, Akamai, etc.
- **[CLI Usage](cli-usage.md)** - Complete command-line interface reference
- **[Security Guide](security.md)** - Security features and best practices
- **[Adding Providers](adding-providers.md)** - Developer guide for adding new providers
- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions
- **[Performance Tuning](performance-tuning.md)** - Optimization recommendations

## Universal Schema

All providers map their log formats to a common universal schema with these required fields:

- `timestamp` - Request timestamp (UTC)
- `client_ip` - Client IP address
- `method` - HTTP method (GET, POST, etc.)
- `host` - Host header / domain
- `path` - Request URI path
- `status_code` - HTTP response status code
- `user_agent` - User-Agent header

Optional fields include query strings, response sizes, cache status, edge locations, and more.

See [Universal Format Specification](universal-format.md) for complete details.

## Next Steps

1. Choose your provider and follow the [provider-specific guide](providers/)
2. Export your logs in the supported format
3. Run ingestion with the CLI
4. Verify data in your database

For detailed information, see the specific documentation pages linked above.

