# Troubleshooting Guide

## Common Issues and Solutions

### File Not Found

**Error**: `File does not exist: /path/to/logs.csv`

**Solutions**:
1. Check the file path is correct
2. Verify file permissions (read access)
3. Use absolute paths if relative paths fail
4. Check file hasn't been moved or deleted

**Example**:
```bash
# Check file exists
ls -l /path/to/logs.csv

# Check permissions
chmod 644 /path/to/logs.csv
```

### Missing Required Fields

**Error**: `Missing required field: timestamp`

**Solutions**:
1. Ensure all required fields are present in your log file
2. Check field names match exactly (case-sensitive)
3. Verify CSV headers or JSON field names match universal schema
4. Use `--validate-only` to check before ingestion

**Required Fields**:
- `timestamp`
- `client_ip`
- `method`
- `host`
- `path`
- `status_code`
- `user_agent`

### Invalid Timestamp Format

**Error**: `Invalid timestamp format: ...`

**Solutions**:
1. Use ISO 8601 format: `2024-01-15T12:30:45+00:00`
2. Or use Unix timestamp (seconds): `1705324245`
3. Ensure timestamps are UTC or include timezone
4. Check for invalid date values (e.g., `2024-13-45`)

**Example**:
```csv
# Good
timestamp,client_ip,...
2024-01-15T12:30:45+00:00,192.0.2.100,...

# Also good
timestamp,client_ip,...
1705324245,192.0.2.100,...

# Bad
timestamp,client_ip,...
Jan 15 2024,192.0.2.100,...  # Unsupported format
```

### Provider Not Found

**Error**: `Unknown provider: 'your_provider'`

**Solutions**:
1. Check provider name spelling (case-sensitive)
2. List available providers: `python scripts/ingest_logs.py --list-providers`
3. Use correct provider name: `universal`, `aws_cloudfront`, `cloudflare`
4. Ensure provider adapter is registered

**Example**:
```bash
# List providers
python scripts/ingest_logs.py --list-providers

# Use correct provider name
python scripts/ingest_logs.py --provider universal --input logs.csv
```

### Unsupported Source Type

**Error**: `Unsupported source type: 'api' for provider 'universal'`

**Solutions**:
1. Check provider supports the source type
2. Use correct source type for provider
3. See provider documentation for supported types

**Supported Types**:
- `universal`: `csv_file`, `tsv_file`, `json_file`, `ndjson_file`
- `aws_cloudfront`: `w3c_file`
- `cloudflare`: `api`, `csv_file`, `json_file`, `ndjson_file`

### Parse Errors

**Error**: `Parse error in file logs.csv at line 42: ...`

**Solutions**:
1. Check file encoding (should be UTF-8)
2. Verify CSV delimiter matches format (comma for CSV, tab for TSV)
3. Check for malformed JSON (use JSON validator)
4. Ensure no trailing commas or missing quotes in CSV
5. Check for special characters that need escaping

**Example**:
```bash
# Validate JSON
python -m json.tool logs.json

# Check CSV format
head -n 5 logs.csv
```

### Database Locked

**Error**: `database is locked`

**Solutions**:
1. Close other connections to the database
2. Wait for other processes to finish
3. Use a different database path for parallel processing
4. Check for stale lock files

**Example**:
```bash
# Check for other processes
lsof data/llm-bot-logs.db

# Use different database
python scripts/ingest_logs.py --db-path /tmp/test.db --input logs.csv
```

### Memory Issues

**Error**: `MemoryError` or `Out of memory`

**Solutions**:
1. Process files individually instead of directories
2. Use smaller batch sizes: `--batch-size 100`
3. Filter data early: `--start-date` and `--end-date`
4. Use NDJSON format for large files (more memory-efficient)
5. Process compressed files (smaller on disk)

**Example**:
```bash
# Smaller batch size
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --batch-size 100

# Process one file at a time
for file in logs/*.csv; do
  python scripts/ingest_logs.py --provider universal --input "$file"
done
```

### Cloudflare API Errors

**Error**: `Invalid API token` or `Unauthorized`

**Solutions**:
1. Verify API token has "Zone Logs:Read" permission
2. Check token is set in environment or config
3. Ensure zone ID is correct
4. Check token hasn't expired

**Example**:
```bash
# Check environment variables
echo $CLOUDFLARE_API_TOKEN
echo $CLOUDFLARE_ZONE_ID

# Set if missing
export CLOUDFLARE_API_TOKEN="your-token"
export CLOUDFLARE_ZONE_ID="your-zone-id"
```

### Time Range Errors

**Error**: `Invalid time range: start_time > end_time`

**Solutions**:
1. Ensure start date is before end date
2. Check date format is correct
3. Use ISO 8601 format: `2024-01-15T12:00:00`

**Example**:
```bash
# Correct
python scripts/ingest_logs.py \
  --start-date 2024-01-15T00:00:00 \
  --end-date 2024-01-16T00:00:00

# Wrong (start > end)
python scripts/ingest_logs.py \
  --start-date 2024-01-16T00:00:00 \
  --end-date 2024-01-15T00:00:00
```

### Empty Directory

**Error**: `No matching files found in directory`

**Solutions**:
1. Check directory contains files with correct extensions
2. Verify file extensions match provider format
3. Check for hidden files or subdirectories
4. Ensure files are readable

**Example**:
```bash
# List files in directory
ls -la logs/

# Check file extensions
find logs/ -name "*.csv" -o -name "*.json"
```

### Gzip Decompression Errors

**Error**: `Not a gzip file` or `Corrupt gzip file`

**Solutions**:
1. Verify file is actually gzip-compressed
2. Check file isn't corrupted
3. Try decompressing manually: `gunzip -t file.gz`
4. Re-download or re-export the file

**Example**:
```bash
# Test gzip file
gunzip -t logs.csv.gz

# Decompress manually if needed
gunzip logs.csv.gz
```

## Security Errors

### Path Traversal Detected

**Error**: `Path traversal detected: ../../../etc/passwd`

**Cause**: The input path contains directory traversal sequences (`..`) that could escape the intended directory.

**Solutions**:
1. Use absolute paths to files within the intended directory
2. Remove `..` sequences from paths
3. When using `--base-dir`, ensure all paths are within that directory

**Example**:
```bash
# Wrong - contains traversal
python scripts/ingest_logs.py --input ../../../etc/passwd

# Correct - use absolute path
python scripts/ingest_logs.py --input /var/logs/cdn/access.log

# Correct - with base-dir restriction
python scripts/ingest_logs.py \
  --input /var/logs/cdn/access.log \
  --base-dir /var/logs/cdn
```

### Path Escapes Base Directory

**Error**: `Path escapes base directory: /etc/passwd is not within /var/logs`

**Cause**: When `--base-dir` is specified, the resolved path must be within that directory.

**Solutions**:
1. Ensure the input path is within the base directory
2. Check for symlinks that might point outside the base directory
3. Use a broader base directory if needed

**Example**:
```bash
# This will fail
python scripts/ingest_logs.py \
  --input /tmp/logs/access.log \
  --base-dir /var/logs

# This will work
python scripts/ingest_logs.py \
  --input /var/logs/cdn/access.log \
  --base-dir /var/logs
```

### File Too Large

**Error**: `File size (15.2 GB) exceeds maximum limit (10.0 GB)`

**Cause**: The file exceeds the configured maximum size limit.

**Solutions**:
1. Increase the limit with `--max-file-size`
2. Split the file into smaller chunks
3. Use compressed files (gzip) which are smaller

**Example**:
```bash
# Increase limit to 20GB
python scripts/ingest_logs.py \
  --input huge-log.csv \
  --max-file-size 21474836480  # 20GB in bytes

# Or split the file first
split -l 10000000 huge-log.csv split_
for f in split_*; do
  python scripts/ingest_logs.py --input "$f"
done
```

### Rate Limit Exceeded

**Error**: `Rate limit exceeded for cloudflare_api`

**Cause**: Too many API requests in the configured time window.

**Solutions**:
1. Wait for the rate limit window to reset (typically 60 seconds)
2. Reduce the frequency of API calls
3. Use file-based ingestion instead of API for bulk operations

**Example**:
```bash
# Use file export instead of API
# 1. Export logs to file from Cloudflare dashboard
# 2. Ingest from file
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input cloudflare-export.json
```

### Field Too Long

**Error**: `Field 'user_agent' exceeds maximum length: 5000 > 2048`

**Cause**: A field value exceeds the maximum allowed length.

**Solutions**:
1. Pre-process data to truncate oversized fields
2. Check for corrupted or malformed data
3. Review the data source for anomalies

**Example**:
```bash
# Check for long user agents
awk -F, '{if(length($7) > 2048) print NR": "length($7)}' logs.csv
```

### Invalid Encoding

**Error**: `Invalid utf-8 encoding at position 1234: invalid start byte`

**Cause**: The file contains bytes that are not valid UTF-8.

**Solutions**:
1. Convert the file to UTF-8
2. Check for binary content mixed with text
3. Use a tool like `iconv` to fix encoding

**Example**:
```bash
# Check encoding
file --mime-encoding logs.csv

# Convert to UTF-8
iconv -f ISO-8859-1 -t UTF-8 logs.csv > logs_utf8.csv
```

## Debugging Tips

### Enable Verbose Logging

```bash
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --verbose
```

### Validate Before Ingestion

```bash
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --validate-only \
  --verbose
```

### Check File Format

```bash
# CSV
head -n 5 logs.csv

# JSON
head -n 5 logs.json | python -m json.tool

# NDJSON
head -n 5 logs.ndjson | python -m json.tool
```

### Test Provider Detection

```bash
# Auto-detect provider
python scripts/ingest_logs.py --input logs.csv --verbose
```

### Check Database

```bash
# SQLite
sqlite3 data/llm-bot-logs.db "SELECT COUNT(*) FROM raw_bot_requests;"

# Check recent records
sqlite3 data/llm-bot-logs.db "SELECT * FROM raw_bot_requests LIMIT 10;"
```

## Getting Help

If you encounter an issue not covered here:

1. **Check logs**: Review error messages and stack traces
2. **Validate input**: Use `--validate-only` to check files
3. **Test with sample data**: Try with a small sample file
4. **Check documentation**: Review provider-specific guides
5. **Open an issue**: Report bugs with error messages and sample data

## See Also

- [CLI Usage Reference](cli-usage.md)
- [Universal Format Specification](universal-format.md)
- [Provider Guides](providers/)
- [Performance Tuning Guide](performance-tuning.md)
- [Security Guide](security.md)

