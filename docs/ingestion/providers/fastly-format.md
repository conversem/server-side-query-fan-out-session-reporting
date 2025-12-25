# Fastly Log Streaming Format

This guide covers ingesting Fastly real-time log streaming data using the native `fastly` adapter.

## Quick Start

```bash
# Ingest Fastly JSON logs
python scripts/ingest_logs.py \
  --provider fastly \
  --input fastly-logs/

# Ingest NDJSON format
python scripts/ingest_logs.py \
  --provider fastly \
  --source-type fastly_ndjson_file \
  --input fastly-logs/access.ndjson

# Ingest CSV format
python scripts/ingest_logs.py \
  --provider fastly \
  --source-type fastly_csv_file \
  --input fastly-logs/access.csv

# With bot filtering
python scripts/ingest_logs.py \
  --provider fastly \
  --input fastly-logs/ \
  --filter-bots
```

---

## Configuring Fastly Log Streaming

### Step 1: Create Log Endpoint

1. Go to [Fastly Console](https://manage.fastly.com/)
2. Select your service → **Logging** → **Create endpoint**
3. Choose destination (S3, GCS, BigQuery, etc.)

### Step 2: Configure Log Format

Use JSON or NDJSON format with these recommended fields:

**Recommended JSON Format String:**
```
{
  "timestamp": "%{begin:%Y-%m-%dT%H:%M:%SZ}t",
  "client_ip": "%h",
  "method": "%m",
  "host": "%{Host}i",
  "path": "%U",
  "query_string": "%q",
  "status_code": %s,
  "user_agent": "%{User-Agent}i",
  "response_bytes": %b,
  "response_time_ms": %D,
  "cache_status": "%{Fastly-Cache-Status}o",
  "pop": "%{server.datacenter}V"
}
```

### Step 3: Download Logs

```bash
# From S3
aws s3 sync s3://your-bucket/fastly-logs/ ./fastly-logs/

# From GCS
gsutil -m rsync -r gs://your-bucket/fastly-logs/ ./fastly-logs/
```

---

## Supported Formats

| Source Type | File Extensions | Description |
|-------------|-----------------|-------------|
| `fastly_json_file` | `.json`, `.json.gz` | JSON array or single object |
| `fastly_ndjson_file` | `.ndjson`, `.jsonl`, `.ndjson.gz` | Newline-delimited JSON |
| `fastly_csv_file` | `.csv`, `.csv.gz` | CSV with header row |

All formats support gzip compression (automatically detected).

---

## Field Mapping

The adapter automatically handles common Fastly field name variations:

### Default Field Mapping

| Universal Field | Fastly Fields (checked in order) |
|-----------------|-----------------------------------|
| `timestamp` | `timestamp`, `time`, `request_time`, `start_time` |
| `client_ip` | `client_ip`, `clientip`, `client`, `ip`, `remote_addr` |
| `method` | `method`, `http_method`, `request_method`, `verb` |
| `host` | `host`, `hostname`, `server_name`, `domain` |
| `path` | `path`, `uri`, `url`, `request_uri`, `request_path` |
| `status_code` | `status_code`, `status`, `http_status`, `response_code` |
| `user_agent` | `user_agent`, `useragent`, `ua` |
| `response_bytes` | `response_bytes`, `bytes`, `body_bytes`, `size` |
| `response_time_ms` | `response_time_ms`, `response_time`, `duration`, `latency` |
| `cache_status` | `cache_status`, `cache_hit`, `x_cache` |
| `edge_location` | `edge_location`, `pop`, `datacenter` |

### Timestamp Formats

The adapter handles multiple timestamp formats:

| Format | Example |
|--------|---------|
| ISO 8601 | `2024-01-15T12:30:45Z` |
| ISO 8601 with offset | `2024-01-15T12:30:45+00:00` |
| Unix seconds | `1705320645` |
| Unix milliseconds | `1705320645000` |

---

## Sample Log Records

### JSON Format

```json
[
  {
    "timestamp": "2024-01-15T12:30:45Z",
    "client_ip": "192.0.2.100",
    "method": "GET",
    "host": "example.com",
    "path": "/api/data",
    "query_string": "key=value",
    "status_code": 200,
    "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
    "response_bytes": 1024,
    "response_time_ms": 150,
    "cache_status": "HIT",
    "pop": "SFO"
  }
]
```

### NDJSON Format

```json
{"timestamp":"2024-01-15T12:30:45Z","client_ip":"192.0.2.100","method":"GET","host":"example.com","path":"/api/data","status_code":200,"user_agent":"Mozilla/5.0 (compatible; GPTBot/1.0)"}
{"timestamp":"2024-01-15T12:30:46Z","client_ip":"192.0.2.101","method":"POST","host":"api.example.com","path":"/submit","status_code":201,"user_agent":"ClaudeBot/1.0"}
```

### CSV Format

```csv
timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api/data,200,"Mozilla/5.0 (compatible; GPTBot/1.0)"
2024-01-15T12:30:46Z,192.0.2.101,POST,api.example.com,/submit,201,ClaudeBot/1.0
```

---

## Usage Examples

### Basic Ingestion

```bash
# Ingest all JSON files from directory
python scripts/ingest_logs.py \
  --provider fastly \
  --input fastly-logs/
```

### With Time Filtering

```bash
# Ingest logs from specific date range
python scripts/ingest_logs.py \
  --provider fastly \
  --input fastly-logs/ \
  --start-time 2024-01-01 \
  --end-time 2024-01-31
```

### Filter LLM Bot Traffic

```bash
# Only ingest LLM bot requests
python scripts/ingest_logs.py \
  --provider fastly \
  --input fastly-logs/ \
  --filter-bots
```

### Validate Before Ingestion

```bash
# Dry run to validate format
python scripts/ingest_logs.py \
  --provider fastly \
  --input fastly-logs/ \
  --dry-run
```

---

## Custom Field Mapping

If your Fastly logs use non-standard field names, configure custom mapping:

```python
from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

adapter = get_adapter("fastly")
source = IngestionSource(
    provider="fastly",
    source_type="fastly_json_file",
    path_or_uri="/path/to/logs.json",
    options={
        "field_mapping": {
            "timestamp": "request_time",    # Your field name
            "client_ip": "clientip",        # Your field name
            "status_code": "http_status",   # Your field name
        }
    }
)

for record in adapter.ingest(source):
    print(record)
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Empty records | Field names don't match | Check your log format configuration |
| Missing timestamps | Non-standard format | Use ISO 8601 or Unix timestamps |
| Wrong source type | CSV vs JSON confusion | Use correct `--source-type` flag |
| Gzip errors | Corrupted file | Re-download from source |

### Verifying Log Format

Check your log format in Fastly Console:
1. Go to your service → **Logging**
2. Click on your endpoint
3. Review the **Log format** field

### Field Name Issues

If fields aren't being mapped correctly:

```bash
# Check first few lines of your log
head -5 fastly-logs/access.json | jq .

# Look for actual field names used
```

---

## Performance

| Metric | Value |
|--------|-------|
| JSON throughput | ~12,000-126,000 records/second |
| NDJSON throughput | ~12,000-126,000 records/second |
| CSV throughput | ~12,000-126,000 records/second |
| Memory | Streaming (constant memory usage) |
| Gzip support | Automatic decompression |

---

## Fastly Format Variables Reference

Common Fastly format variables for log configuration:

| Variable | Description |
|----------|-------------|
| `%h` | Client IP address |
| `%m` | HTTP method |
| `%U` | URI path |
| `%q` | Query string |
| `%s` | Status code |
| `%b` | Response bytes |
| `%D` | Response time (microseconds) |
| `%{begin:%Y-%m-%dT%H:%M:%SZ}t` | Request timestamp (ISO 8601) |
| `%{Host}i` | Host header |
| `%{User-Agent}i` | User-Agent header |
| `%{Referer}i` | Referer header |
| `%{Fastly-Cache-Status}o` | Cache status |
| `%{server.datacenter}V` | POP/datacenter |

---

## See Also

- [Fastly Real-Time Log Streaming](https://docs.fastly.com/en/guides/about-fastly-log-streaming)
- [Fastly Log Format Variables](https://developer.fastly.com/reference/vcl/variables/)
- [CLI Usage Guide](../cli-usage.md)
- [Other Providers](other-providers.md)
