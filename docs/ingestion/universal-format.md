# Universal Format Specification

## Overview

The universal format is a standardized schema for log ingestion that works across all CDN and cloud providers. Logs in this format can be ingested using the `universal` provider adapter.

## Supported Formats

- **CSV** (`.csv`) - Comma-separated values with header row
- **TSV** (`.tsv`) - Tab-separated values with header row
- **JSON** (`.json`) - Single JSON object or array of objects
- **NDJSON** (`.ndjson`, `.jsonl`) - Newline-delimited JSON (JSON Lines)

All formats support gzip compression (`.gz` extension).

## Required Fields

These fields **must** be present in every log record:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `timestamp` | datetime/string | Request timestamp (UTC) | `2024-01-15T12:30:45+00:00` |
| `client_ip` | string | Client IP address (IPv4 or IPv6) | `192.0.2.100` |
| `method` | string | HTTP method | `GET`, `POST`, `PUT`, `DELETE` |
| `host` | string | Host header / domain | `example.com` |
| `path` | string | Request URI path | `/api/data` |
| `status_code` | integer | HTTP response status code | `200`, `404`, `500` |
| `user_agent` | string | User-Agent header | `Mozilla/5.0...` |

## Optional Fields

These fields may be included but are not required:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `query_string` | string | Query parameters (without `?`) | `key=value&foo=bar` |
| `response_bytes` | integer | Response body size in bytes | `1024` |
| `request_bytes` | integer | Request body size in bytes | `256` |
| `response_time_ms` | integer | Response latency in milliseconds | `150` |
| `cache_status` | string | Cache hit/miss status | `HIT`, `MISS`, `BYPASS` |
| `edge_location` | string | Edge POP identifier | `LAX`, `DFW`, `SFO` |
| `referer` | string | Referer header | `https://example.com/referer` |
| `protocol` | string | HTTP protocol version | `HTTP/1.1`, `HTTP/2` |
| `ssl_protocol` | string | TLS version | `TLSv1.3`, `TLSv1.2` |

## Timestamp Formats

The `timestamp` field accepts multiple formats:

- **ISO 8601**: `2024-01-15T12:30:45+00:00` or `2024-01-15T12:30:45Z`
- **Unix timestamp** (seconds): `1705324245`
- **Unix timestamp** (milliseconds): `1705324245000`
- **Unix timestamp** (microseconds): `1705324245000000`
- **Unix timestamp** (nanoseconds): `1705324245000000000`
- **Common log format**: `15/Jan/2024:12:30:45 +0000`

## Format Examples

### CSV Format

```csv
timestamp,client_ip,method,host,path,status_code,user_agent,query_string,response_bytes,request_bytes,response_time_ms,cache_status,edge_location,referer,protocol,ssl_protocol
2024-01-15T12:30:45+00:00,192.0.2.100,GET,example.com,/api/data,200,Mozilla/5.0 (compatible; GPTBot/1.0),?key=value,1024,256,150,HIT,LAX,https://example.com/referer,HTTP/1.1,TLSv1.3
2024-01-15T12:30:46+00:00,192.0.2.101,POST,example.com,/api/submit,201,Mozilla/5.0 (compatible; ChatGPT-User/1.0),,2048,512,200,MISS,DFW,,HTTP/2,TLSv1.3
```

### TSV Format

```tsv
timestamp	client_ip	method	host	path	status_code	user_agent	query_string	response_bytes	request_bytes	response_time_ms	cache_status	edge_location	referer	protocol	ssl_protocol
2024-01-15T12:30:45+00:00	192.0.2.100	GET	example.com	/api/data	200	Mozilla/5.0 (compatible; GPTBot/1.0)	key=value	1024	256	150	HIT	LAX	https://example.com/referer	HTTP/1.1	TLSv1.3
```

### JSON Format (Array)

```json
[
  {
    "timestamp": "2024-01-15T12:30:45+00:00",
    "client_ip": "192.0.2.100",
    "method": "GET",
    "host": "example.com",
    "path": "/api/data",
    "status_code": 200,
    "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
    "query_string": "key=value",
    "response_bytes": 1024,
    "request_bytes": 256,
    "response_time_ms": 150,
    "cache_status": "HIT",
    "edge_location": "LAX",
    "referer": "https://example.com/referer",
    "protocol": "HTTP/1.1",
    "ssl_protocol": "TLSv1.3"
  },
  {
    "timestamp": "2024-01-15T12:30:46+00:00",
    "client_ip": "192.0.2.101",
    "method": "POST",
    "host": "example.com",
    "path": "/api/submit",
    "status_code": 201,
    "user_agent": "Mozilla/5.0 (compatible; ChatGPT-User/1.0)",
    "response_bytes": 2048,
    "request_bytes": 512,
    "response_time_ms": 200,
    "cache_status": "MISS",
    "edge_location": "DFW",
    "protocol": "HTTP/2",
    "ssl_protocol": "TLSv1.3"
  }
]
```

### NDJSON Format (JSON Lines)

```jsonl
{"timestamp":"2024-01-15T12:30:45+00:00","client_ip":"192.0.2.100","method":"GET","host":"example.com","path":"/api/data","status_code":200,"user_agent":"Mozilla/5.0 (compatible; GPTBot/1.0)","query_string":"key=value","response_bytes":1024,"request_bytes":256,"response_time_ms":150,"cache_status":"HIT","edge_location":"LAX","referer":"https://example.com/referer","protocol":"HTTP/1.1","ssl_protocol":"TLSv1.3"}
{"timestamp":"2024-01-15T12:30:46+00:00","client_ip":"192.0.2.101","method":"POST","host":"example.com","path":"/api/submit","status_code":201,"user_agent":"Mozilla/5.0 (compatible; ChatGPT-User/1.0)","response_bytes":2048,"request_bytes":512,"response_time_ms":200,"cache_status":"MISS","edge_location":"DFW","protocol":"HTTP/2","ssl_protocol":"TLSv1.3"}
```

## Field Validation

### Timestamp

- Must be a valid datetime or parseable timestamp string
- Automatically converted to UTC timezone
- Supports ISO 8601, Unix timestamps (seconds/milliseconds/microseconds/nanoseconds), and common log formats

### IP Address

- Must be a valid IPv4 or IPv6 address
- Examples: `192.0.2.100`, `2001:db8::1`

### HTTP Method

- Must be a valid HTTP method: `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`, `TRACE`, `CONNECT`

### Status Code

- Must be a valid HTTP status code (100-599)
- Common values: `200`, `201`, `301`, `302`, `400`, `401`, `403`, `404`, `500`, `502`, `503`

### Query String

- Should not include the leading `?`
- URL-encoded values are preserved as-is
- Example: `key=value&foo=bar` (not `?key=value&foo=bar`)

## Usage

### CSV File

```bash
python scripts/ingest_logs.py --provider universal --input logs.csv
```

### JSON File

```bash
python scripts/ingest_logs.py --provider universal --input logs.json
```

### NDJSON File

```bash
python scripts/ingest_logs.py --provider universal --input logs.ndjson
```

### Gzip Compressed

```bash
python scripts/ingest_logs.py --provider universal --input logs.csv.gz
```

### Directory

```bash
python scripts/ingest_logs.py --provider universal --input logs/
```

## Best Practices

1. **Use consistent field names**: Field names must match exactly (case-sensitive)
2. **Include all required fields**: Missing required fields will cause validation errors
3. **Use UTC timestamps**: All timestamps are normalized to UTC
4. **Handle missing optional fields**: Use empty strings or omit fields entirely
5. **Validate before ingestion**: Use `--validate-only` flag to check files before processing
6. **Use NDJSON for large files**: NDJSON format is more memory-efficient for large datasets

## Common Issues

### Missing Required Fields

**Error**: `Missing required field: timestamp`

**Solution**: Ensure all required fields are present in every record.

### Invalid Timestamp Format

**Error**: `Invalid timestamp format: ...`

**Solution**: Use ISO 8601 format (`2024-01-15T12:30:45+00:00`) or Unix timestamp.

### Field Name Mismatch

**Error**: `Missing required field mappings`

**Solution**: Ensure CSV headers or JSON field names match the universal schema exactly (case-sensitive).

See [Troubleshooting Guide](troubleshooting.md) for more solutions.


