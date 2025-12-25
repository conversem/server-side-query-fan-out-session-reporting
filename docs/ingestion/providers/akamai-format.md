# Akamai DataStream 2 Log Format

This guide covers ingesting Akamai DataStream 2 logs using the native `akamai` adapter.

## Quick Start

```bash
# Ingest Akamai JSON logs
python scripts/ingest_logs.py \
  --provider akamai \
  --input akamai-logs/

# Ingest NDJSON format
python scripts/ingest_logs.py \
  --provider akamai \
  --source-type akamai_ndjson_file \
  --input akamai-logs/datastream.ndjson

# With bot filtering
python scripts/ingest_logs.py \
  --provider akamai \
  --input akamai-logs/ \
  --filter-bots
```

---

## Configuring Akamai DataStream 2

### Step 1: Create DataStream

1. Go to Akamai Control Center
2. Navigate to **Delivery** → **DataStream**
3. Click **Create new stream**

### Step 2: Select Destination

Choose from supported destinations:
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- Custom HTTPS endpoint

### Step 3: Configure Fields

Select the fields to include in your logs. Recommended fields:

| Field | Description |
|-------|-------------|
| Request Time | Timestamp of the request |
| Client IP | Client IP address |
| Request Method | HTTP method |
| Request Host | Host header |
| Request Path | URI path |
| Response Status | HTTP status code |
| User-Agent | User-Agent header |
| Query String | Query parameters |
| Bytes | Response size |
| Turnaround Time | Response time in ms |
| Cache Status | HIT/MISS status |
| TLS Version | SSL/TLS protocol |

### Step 4: Download Logs

```bash
# From S3
aws s3 sync s3://your-datastream-bucket/ ./akamai-logs/

# From GCS
gsutil -m rsync -r gs://your-datastream-bucket/ ./akamai-logs/
```

---

## Supported Formats

| Source Type | File Extensions | Description |
|-------------|-----------------|-------------|
| `akamai_json_file` | `.json`, `.json.gz` | JSON array or single object |
| `akamai_ndjson_file` | `.ndjson`, `.jsonl`, `.ndjson.gz` | Newline-delimited JSON |

All formats support gzip compression (automatically detected).

---

## Field Mapping

The adapter automatically maps Akamai camelCase fields to the universal schema:

### Required Fields

| Akamai Field | Universal Field | Notes |
|--------------|-----------------|-------|
| `requestTime` | `timestamp` | ISO 8601 or Unix epoch |
| `clientIP` | `client_ip` | Client IP address |
| `requestMethod` | `method` | HTTP method |
| `requestHost` | `host` | Host header |
| `requestPath` | `path` | URI path |
| `responseStatus` | `status_code` | HTTP status code |
| `userAgent` | `user_agent` | User-Agent header |

### Optional Fields

| Akamai Field | Universal Field | Notes |
|--------------|-----------------|-------|
| `bytes` / `totalBytes` | `response_bytes` | Response size |
| `turnaroundTimeMs` | `response_time_ms` | Response time |
| `queryString` | `query_string` | Query parameters |
| `tlsVersion` | `ssl_protocol` | TLS version |
| `cacheStatus` | `cache_status` | HIT/MISS |
| `edgeServerIP` | `edge_location` | Edge server |
| `requestProtocol` | `protocol` | HTTP version |
| `referer` | `referer` | Referer header |

### Field Name Aliases

The adapter checks multiple field name variations:

| Universal Field | Checked Fields (in order) |
|-----------------|---------------------------|
| `timestamp` | `requestTime`, `reqTimeSec`, `timestamp`, `time` |
| `client_ip` | `clientIP`, `clientIp`, `client_ip`, `cliIP` |
| `method` | `requestMethod`, `reqMethod`, `method`, `httpMethod` |
| `status_code` | `responseStatus`, `resStatus`, `status`, `statusCode` |
| `response_bytes` | `bytes`, `totalBytes`, `respBytes`, `size` |
| `response_time_ms` | `turnaroundTimeMs`, `transferTimeMs`, `latency`, `ttms` |

---

## Timestamp Formats

The adapter handles multiple timestamp formats:

| Format | Example | Detection |
|--------|---------|-----------|
| ISO 8601 (UTC) | `2024-01-15T12:30:45.123Z` | Contains 'T' and 'Z' |
| ISO 8601 (offset) | `2024-01-15T12:30:45+00:00` | Contains 'T' and offset |
| Unix seconds | `1705320645` | 10-digit integer |
| Unix milliseconds | `1705320645123` | 13-digit integer |

---

## Sample Log Records

### JSON Format (Array)

```json
[
  {
    "requestTime": "2024-01-15T12:30:45.123Z",
    "clientIP": "192.0.2.100",
    "requestMethod": "GET",
    "requestHost": "example.com",
    "requestPath": "/api/data",
    "queryString": "key=value",
    "responseStatus": 200,
    "userAgent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
    "bytes": 1024,
    "turnaroundTimeMs": 150,
    "cacheStatus": "HIT",
    "tlsVersion": "TLSv1.3"
  }
]
```

### NDJSON Format

```json
{"requestTime":"2024-01-15T12:30:45.123Z","clientIP":"192.0.2.100","requestMethod":"GET","requestHost":"example.com","requestPath":"/api/data","responseStatus":200,"userAgent":"Mozilla/5.0 (compatible; GPTBot/1.0)"}
{"requestTime":"2024-01-15T12:30:46.000Z","clientIP":"192.0.2.101","requestMethod":"POST","requestHost":"api.example.com","requestPath":"/submit","responseStatus":201,"userAgent":"ClaudeBot/1.0"}
```

### Unix Timestamp Format

```json
{
  "reqTimeSec": 1705320645,
  "clientIP": "192.0.2.100",
  "requestMethod": "GET",
  "requestHost": "example.com",
  "requestPath": "/api/data",
  "responseStatus": 200,
  "userAgent": "Mozilla/5.0 (compatible; GPTBot/1.0)"
}
```

---

## Usage Examples

### Basic Ingestion

```bash
# Ingest all JSON files from directory
python scripts/ingest_logs.py \
  --provider akamai \
  --input akamai-logs/
```

### With Time Filtering

```bash
# Ingest logs from specific date range
python scripts/ingest_logs.py \
  --provider akamai \
  --input akamai-logs/ \
  --start-time 2024-01-01 \
  --end-time 2024-01-31
```

### Filter LLM Bot Traffic

```bash
# Only ingest LLM bot requests
python scripts/ingest_logs.py \
  --provider akamai \
  --input akamai-logs/ \
  --filter-bots
```

### Validate Before Ingestion

```bash
# Dry run to validate format
python scripts/ingest_logs.py \
  --provider akamai \
  --input akamai-logs/ \
  --dry-run
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Empty records | Field names don't match | Check your DataStream configuration |
| Missing timestamps | Non-standard format | Ensure ISO 8601 or Unix epoch |
| Wrong source type | NDJSON vs JSON array | Use correct `--source-type` flag |
| Gzip errors | Corrupted file | Re-download from source |

### Verifying Log Format

Check your DataStream configuration in Akamai Control Center:
1. Go to **Delivery** → **DataStream**
2. Select your stream
3. Review the **Data Set** configuration

### Field Name Issues

If fields aren't being mapped correctly, check actual field names:

```bash
# Check first few lines of your log
head -5 akamai-logs/datastream.json | jq .

# For NDJSON
head -5 akamai-logs/datastream.ndjson | jq .
```

### Unix Timestamp Issues

The adapter auto-detects Unix timestamps:
- 10-digit numbers are treated as **seconds**
- 13-digit numbers are treated as **milliseconds**

If timestamps are incorrect, check your DataStream timestamp format configuration.

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

- [Akamai DataStream 2 Documentation](https://techdocs.akamai.com/datastream2/docs)
- [DataStream 2 Data Set Parameters](https://techdocs.akamai.com/datastream2/docs/data-set-parameters)
- [CLI Usage Guide](../cli-usage.md)
- [Other Providers](other-providers.md)
