# CLI Usage Reference

## Command Overview

```bash
python scripts/ingest_logs.py [OPTIONS]
```

## Options

### Required Options

- **`--input PATH`** - Path to log file or directory (required unless `--list-providers`)

### Provider Options

- **`--provider PROVIDER`** - Provider name (optional, auto-detected if omitted)
  - Values: `universal`, `aws_cloudfront`, `aws_alb`, `azure_cdn`, `cloudflare`, `fastly`, `akamai`, `gcp_cdn`
  - Default: Auto-detected from file format

- **`--list-providers`** - List all available providers and exit

### Filtering Options

- **`--filter-bots`** - Filter for LLM bot traffic only (GPTBot, ChatGPT-User, ClaudeBot, etc.)
  - Default: `True` (bot filtering enabled by default)
  - Note: This is the default behavior. Use `--no-filter-bots` to disable.

- **`--no-filter-bots`** - Disable bot filtering (include all traffic)
  - Use this flag to ingest all traffic, not just LLM bots

- **`--start-date DATETIME`** - Start time filter (ISO 8601 or YYYY-MM-DD)
  - Format: `2024-01-15` or `2024-01-15T12:00:00`
  - Default: No start filter

- **`--end-date DATETIME`** - End time filter (ISO 8601 or YYYY-MM-DD)
  - Format: `2024-01-15` or `2024-01-15T12:00:00`
  - Default: No end filter

### Processing Options

- **`--validate-only`** - Validate files without inserting into database
  - Useful for testing before full ingestion

- **`--batch-size SIZE`** - Records per batch for database insertion
  - Default: `1000`
  - Range: `1` to `10000`

- **`--verbose`** - Enable verbose logging
  - Shows detailed progress and debug information

### Database Options

- **`--db-path PATH`** - Path to SQLite database file
  - Default: `data/llm-bot-logs.db`

### Security Options

- **`--base-dir PATH`** - Restrict file access to this directory (prevents path traversal)
  - When set, input paths must be within this directory
  - Helps prevent security issues from malicious file paths

- **`--max-file-size SIZE`** - Maximum allowed file size
  - Default: `10GB`
  - Supports units: B, KB, MB, GB, TB (case-insensitive)
  - Examples: `10GB`, `500MB`, `1TB`
  - Files exceeding this limit will be rejected

## Usage Examples

### Basic Usage

```bash
# Ingest CSV file
python scripts/ingest_logs.py --provider universal --input logs.csv

# Ingest directory
python scripts/ingest_logs.py --provider universal --input logs/

# Auto-detect provider
python scripts/ingest_logs.py --input logs.csv
```

### Provider-Specific Examples

**Universal Format (CSV/JSON/NDJSON):**
```bash
python scripts/ingest_logs.py --provider universal --input logs.csv
python scripts/ingest_logs.py --provider universal --input logs.json
python scripts/ingest_logs.py --provider universal --input logs.ndjson
```

**AWS CloudFront (W3C extended log format):**
```bash
python scripts/ingest_logs.py --provider aws_cloudfront --input cloudfront-logs/
python scripts/ingest_logs.py --provider aws_cloudfront --input E2ABCD1234.2024-01-15.log.gz
```

**AWS ALB (space-separated access logs):**
```bash
python scripts/ingest_logs.py --provider aws_alb --input alb-logs/
python scripts/ingest_logs.py --provider aws_alb --input app.my-alb.log.gz
```

**Azure CDN / Front Door:**
```bash
python scripts/ingest_logs.py --provider azure_cdn --input azure-logs/
python scripts/ingest_logs.py --provider azure_cdn --input frontdoor-access.json
```

**Cloudflare (Logpush or API):**
```bash
# From Logpush NDJSON files
python scripts/ingest_logs.py --provider cloudflare --input logpush.ndjson

# From API (requires CLOUDFLARE_API_TOKEN)
python scripts/ingest_logs.py --provider cloudflare --source-type api --input api://zone-id
```

**Fastly (JSON/NDJSON/CSV):**
```bash
python scripts/ingest_logs.py --provider fastly --input fastly-logs.json
python scripts/ingest_logs.py --provider fastly --source-type fastly_ndjson_file --input fastly.ndjson
python scripts/ingest_logs.py --provider fastly --source-type fastly_csv_file --input fastly.csv
```

**Akamai DataStream 2:**
```bash
python scripts/ingest_logs.py --provider akamai --input datastream.json
python scripts/ingest_logs.py --provider akamai --source-type akamai_ndjson_file --input datastream.ndjson
```

**GCP Cloud CDN:**
```bash
python scripts/ingest_logs.py --provider gcp_cdn --input gcp-cdn-logs/
python scripts/ingest_logs.py --provider gcp_cdn --input cdn-access.json
```

### Filtering

```bash
# Bot filtering (default - no flag needed)
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv

# Disable bot filtering (include all traffic)
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --no-filter-bots

# Time range filtering
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --start-date 2024-01-15T00:00:00 \
  --end-date 2024-01-15T23:59:59

# Combined filtering (bot filtering is default)
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --start-date 2024-01-15 \
  --end-date 2024-01-16
```

### Validation

```bash
# Validate without ingesting
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --validate-only

# Validate with verbose output
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --validate-only \
  --verbose
```

### Performance Tuning

```bash
# Large batch size for better performance
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --batch-size 5000

# Custom database path
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --db-path /path/to/custom.db
```

## Exit Codes

- **0** - Success
- **1** - Error (validation failed, parse error, etc.)
- **2** - Invalid arguments

## Output

### Progress Reporting

The CLI provides real-time progress updates:

```
Ingesting from universal (csv_file)
  Processed 1,000 records...
  Processed 2,000 records...
  Processed 3,000 records...
```

### Summary

After completion, a summary is displayed:

```
Ingestion complete!
  Records processed: 5,000
  Records inserted: 5,000
  Records skipped: 0
  Records failed: 0
  Duration: 12.5 seconds
  Throughput: 400 records/second
```

### Validation Mode

In validation mode, a validation report is displayed:

```
Validation complete!
  Files processed: 1
  Records processed: 5,000
  Records valid: 5,000
  Records invalid: 0
  Errors: 0
  Warnings: 0
```

## Common Patterns

### Daily Ingestion Script

```bash
#!/bin/bash
# Ingest yesterday's logs

YESTERDAY=$(date -u -d '1 day ago' +%Y-%m-%d)
TODAY=$(date -u +%Y-%m-%d)

python scripts/ingest_logs.py \
  --provider cloudflare \
  --input api://zone_id \
  --start-date "${YESTERDAY}T00:00:00" \
  --end-date "${TODAY}T00:00:00" \
  --filter-bots
```

### Batch Processing Multiple Files

```bash
#!/bin/bash
# Process all CSV files in a directory

for file in logs/*.csv; do
  echo "Processing $file..."
  python scripts/ingest_logs.py \
    --provider universal \
    --input "$file" \
    --filter-bots
done
```

### Validation Before Ingestion

```bash
#!/bin/bash
# Validate first, then ingest

if python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --validate-only; then
  echo "Validation passed, ingesting..."
  python scripts/ingest_logs.py \
    --provider universal \
    --input logs.csv \
    --filter-bots
else
  echo "Validation failed, aborting"
  exit 1
fi
```

## Environment Variables

The CLI respects these environment variables:

- **`SQLITE_DB_PATH`** - Default database path (overridden by `--db-path`)
- **`CLOUDFLARE_API_TOKEN`** - Cloudflare API token (for API ingestion)
- **`CLOUDFLARE_ZONE_ID`** - Cloudflare zone ID (for API ingestion)

## Error Handling

### Validation Errors

If validation fails, the CLI exits with code 1 and displays errors:

```
Error: Source validation failed: File does not exist: /path/to/logs.csv
```

### Parse Errors

Parse errors are logged and reported:

```
Error: Parse error in file logs.csv at line 42: Missing required field: timestamp
```

### Database Errors

Database errors are caught and reported:

```
Error: Failed to insert batch: database is locked
```

## Troubleshooting

### Provider Not Found

```
Error: Provider 'xyz' not found. Available providers: universal, aws_cloudfront, aws_alb, azure_cdn, cloudflare, fastly, akamai, gcp_cdn
```

**Solution:** Use `--list-providers` to see available providers, or check spelling.

### Unsupported Source Type

```
Error: Unsupported source type 'json_file' for provider 'aws_alb'
```

**Solution:** Each provider supports specific source types. Use appropriate type:
- ALB: `alb_log_file`
- CloudFront: `w3c_file`
- Fastly: `fastly_json_file`, `fastly_ndjson_file`, `fastly_csv_file`
- Akamai: `akamai_json_file`, `akamai_ndjson_file`

### Empty Results

If ingestion produces zero records:

1. **Check file format matches provider** - Wrong provider may not parse correctly
2. **Check field names** - Custom field names may need mapping
3. **Check time filters** - `--start-date` and `--end-date` may exclude all records
4. **Check bot filter** - Use `--no-filter-bots` to include all traffic

### Auto-Detection Fails

```
Warning: Could not auto-detect provider. Please specify with --provider
```

**Solution:** Specify the provider explicitly:
```bash
python scripts/ingest_logs.py --provider fastly --input logs.json
```

### Performance Issues

For slow ingestion:

1. **Increase batch size:** `--batch-size 5000`
2. **Use gzipped files:** Faster I/O despite decompression overhead
3. **Filter early:** Use `--start-date`/`--end-date` to reduce records
4. **Use NDJSON:** Streaming format is faster than JSON arrays

## Tips

1. **Use `--validate-only` first**: Always validate before full ingestion
2. **Start with small batches**: Use `--batch-size 100` for testing
3. **Use verbose mode**: Add `--verbose` for debugging
4. **Filter early**: Use `--start-date` and `--end-date` to reduce processing time
5. **Monitor progress**: Watch progress output for large files
6. **Let auto-detection work**: Often you don't need to specify `--provider`

## See Also

- [Universal Format Specification](universal-format.md)
- [Provider Guides](providers/)
- [Troubleshooting Guide](troubleshooting.md)
- [Performance Tuning Guide](performance-tuning.md)

