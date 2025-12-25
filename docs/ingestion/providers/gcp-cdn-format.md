# GCP Cloud CDN Log Format

This guide covers ingesting Google Cloud CDN logs using the native `gcp_cdn` adapter.

## Quick Start

```bash
# Ingest GCP Cloud CDN JSON logs
python scripts/ingest_logs.py \
  --provider gcp_cdn \
  --input gcp-cdn-logs/

# Ingest NDJSON format
python scripts/ingest_logs.py \
  --provider gcp_cdn \
  --source-type ndjson_file \
  --input gcp-cdn-logs/streaming-export.ndjson

# With bot filtering
python scripts/ingest_logs.py \
  --provider gcp_cdn \
  --input gcp-cdn-logs/ \
  --filter-bots
```

---

## Exporting Logs from Cloud Logging

### Step 1: Create Log Sink to Cloud Storage

1. **Navigate to Cloud Logging**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Go to **Logging** → **Log Router**

2. **Create Sink**:
   - Click "Create Sink"
   - Name: `cdn-logs-sink`
   - Filter:
     ```
     resource.type="http_load_balancer"
     httpRequest.requestUrl!=""
     ```
   - Destination: Cloud Storage bucket
   - Format: JSON (default)

3. **Grant Permissions**:
   - Grant the log sink service account write access to the bucket

### Step 2: Download Logs

```bash
# Sync from GCS
gsutil -m rsync -r gs://your-log-bucket/cdn-logs/ ./gcp-cdn-logs/

# Or download specific files
gsutil cp gs://your-log-bucket/cdn-logs/2024-01-15/*.json ./gcp-cdn-logs/
```

---

## Supported Formats

| Source Type | File Extensions | Description |
|-------------|-----------------|-------------|
| `json_file` | `.json`, `.json.gz` | JSON array from Cloud Logging export |
| `ndjson_file` | `.ndjson`, `.jsonl`, `.ndjson.gz` | NDJSON from streaming export |

All formats support gzip compression (automatically detected).

---

## Field Mapping

The adapter automatically extracts fields from nested `httpRequest` objects:

### Required Fields

| GCP Field | Universal Field | Notes |
|-----------|-----------------|-------|
| `timestamp` | `timestamp` | RFC3339 format |
| `httpRequest.remoteIp` | `client_ip` | IPv4 or IPv6 |
| `httpRequest.requestMethod` | `method` | HTTP method |
| `httpRequest.requestUrl` | `host`, `path`, `query_string` | URL parsed automatically |
| `httpRequest.status` | `status_code` | HTTP status code |
| `httpRequest.userAgent` | `user_agent` | User-Agent header |

### Optional Fields

| GCP Field | Universal Field | Notes |
|-----------|-----------------|-------|
| `httpRequest.requestSize` | `request_bytes` | May be string or integer |
| `httpRequest.responseSize` | `response_bytes` | May be string or integer |
| `httpRequest.latency` | `response_time_ms` | Duration string converted to ms |
| `httpRequest.cacheHit` | `cache_status` | Boolean → "HIT"/"MISS" |
| `httpRequest.referer` | `referer` | Referer header |
| `httpRequest.protocol` | `protocol` | HTTP version |

### Latency Conversion

GCP uses duration strings that are automatically converted:

| GCP Format | Converted Value |
|------------|-----------------|
| `"0.150s"` | 150 ms |
| `"1.500s"` | 1500 ms |
| `"0.025s"` | 25 ms |

### Cache Status Mapping

GCP uses boolean fields converted to standard status strings:

| GCP Fields | Universal `cache_status` |
|------------|--------------------------|
| `cacheHit: true` | `"HIT"` |
| `cacheHit: false, cacheLookup: true` | `"MISS"` |
| `cacheLookup: false` | `"BYPASS"` or `None` |

---

## Sample Log Records

### JSON Format

```json
{
  "timestamp": "2024-01-15T12:30:45.123456Z",
  "httpRequest": {
    "remoteIp": "192.0.2.100",
    "requestMethod": "GET",
    "requestUrl": "https://example.com/api/data?query=test",
    "status": 200,
    "userAgent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
    "responseSize": "1024",
    "latency": "0.150s",
    "cacheHit": false
  }
}
```

### NDJSON Format

```json
{"timestamp":"2024-01-15T12:30:45.123456Z","httpRequest":{"remoteIp":"192.0.2.100","requestMethod":"GET","requestUrl":"https://example.com/api/data","status":200,"userAgent":"GPTBot/1.0"}}
{"timestamp":"2024-01-15T12:30:46.000000Z","httpRequest":{"remoteIp":"198.51.100.50","requestMethod":"GET","requestUrl":"https://example.com/assets/img.png","status":200,"userAgent":"ClaudeBot/1.0"}}
```

---

## Usage Examples

### Basic Ingestion

```bash
# Ingest all JSON files from directory
python scripts/ingest_logs.py \
  --provider gcp_cdn \
  --input gcp-cdn-logs/
```

### With Time Filtering

```bash
# Ingest logs from specific date range
python scripts/ingest_logs.py \
  --provider gcp_cdn \
  --input gcp-cdn-logs/ \
  --start-time 2024-01-01 \
  --end-time 2024-01-31
```

### Filter LLM Bot Traffic

```bash
# Only ingest LLM bot requests
python scripts/ingest_logs.py \
  --provider gcp_cdn \
  --input gcp-cdn-logs/ \
  --filter-bots
```

### Validate Before Ingestion

```bash
# Dry run to validate format
python scripts/ingest_logs.py \
  --provider gcp_cdn \
  --input gcp-cdn-logs/ \
  --dry-run
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Empty records | Missing `httpRequest` object | Some log entries lack HTTP data - they're skipped |
| Missing host | Relative URLs | Adapter handles relative paths gracefully |
| Wrong latency | Non-standard format | Must use GCP's `"0.150s"` format |
| String numbers | GCP quirk | Adapter handles both string and integer sizes |

### Missing httpRequest Object

Some Cloud Logging entries may lack the `httpRequest` object entirely:

```json
{
  "timestamp": "2024-01-15T12:30:45Z",
  "severity": "INFO",
  "logName": "..."
}
```

These entries are automatically skipped.

### URL Parsing Issues

The `requestUrl` field may contain:
- Full URLs: `https://example.com/api/data`
- Relative paths: `/api/data`

Both are handled automatically. For relative paths, the `host` field is extracted from the URL if available, otherwise left as `None`.

---

## Performance

| Metric | Value |
|--------|-------|
| JSON throughput | ~12,000-120,000 records/second |
| NDJSON throughput | ~12,000-120,000 records/second |
| Memory | Streaming (constant memory usage) |
| Gzip support | Automatic decompression |

---

## See Also

- [Cloud CDN Logging Documentation](https://cloud.google.com/cdn/docs/logging)
- [Cloud Logging HttpRequest](https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#HttpRequest)
- [CLI Usage Guide](../cli-usage.md)
- [Other Providers](other-providers.md)
