# AWS Application Load Balancer (ALB) Log Format

This guide covers ingesting AWS ALB access logs using the native `aws_alb` adapter.

## Quick Start

```bash
# Ingest ALB logs directly
python scripts/ingest_logs.py \
  --provider aws_alb \
  --input alb-logs/

# From a specific gzipped file
python scripts/ingest_logs.py \
  --provider aws_alb \
  --input alb-logs/123456789012_elasticloadbalancing_us-east-1_app.my-alb.log.gz

# With bot filtering
python scripts/ingest_logs.py \
  --provider aws_alb \
  --input alb-logs/ \
  --filter-bots
```

---

## Enabling ALB Access Logs

### Step 1: Create S3 Bucket

```bash
aws s3 mb s3://my-alb-logs-bucket
```

### Step 2: Enable Access Logging

**Via AWS Console:**
1. Go to EC2 Console → Load Balancers
2. Select your ALB
3. Actions → Edit attributes
4. Enable "Access logs"
5. Specify S3 bucket location

**Via AWS CLI:**
```bash
aws elbv2 modify-load-balancer-attributes \
  --load-balancer-arn arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-alb/abc123 \
  --attributes Key=access_logs.s3.enabled,Value=true \
               Key=access_logs.s3.bucket,Value=my-alb-logs-bucket \
               Key=access_logs.s3.prefix,Value=alb-logs
```

### Step 3: Download Logs

```bash
aws s3 sync s3://my-alb-logs-bucket/alb-logs/ ./alb-logs/
```

---

## Supported Formats

| Source Type | File Extensions | Description |
|-------------|-----------------|-------------|
| `alb_log_file` | `.log`, `.log.gz` | ALB space-separated access logs |

The adapter automatically detects and decompresses gzipped files.

---

## Field Mapping

The adapter automatically maps ALB fields to the universal schema:

### Required Fields

| ALB Field | Position | Universal Field | Notes |
|-----------|----------|-----------------|-------|
| `time` | 2 | `timestamp` | ISO 8601 format |
| `client:port` | 4 | `client_ip` | Port stripped automatically |
| `elb_status_code` | 9 | `status_code` | HTTP response status |
| `"request"` | 13 | `method`, `host`, `path`, `query_string` | Parsed from request line |
| `"user_agent"` | 14 | `user_agent` | User-Agent header |

### Optional Fields

| ALB Field | Position | Universal Field | Notes |
|-----------|----------|-----------------|-------|
| `received_bytes` | 11 | `request_bytes` | May be `-` for errors |
| `sent_bytes` | 12 | `response_bytes` | May be `-` for errors |
| `ssl_protocol` | 16 | `ssl_protocol` | TLSv1.2, TLSv1.3, etc. |
| Processing times | 6+7+8 | `response_time_ms` | Sum converted to milliseconds |

### Extra Fields (Preserved in `extra` dict)

| ALB Field | Position | Notes |
|-----------|----------|-------|
| `type` | 1 | http, https, h2, grpcs, ws, wss |
| `elb` | 3 | Load balancer resource ID |
| `target_group_arn` | 17 | Target group ARN |
| `trace_id` | 18 | X-Amzn-Trace-Id header |

---

## Log Format Details

ALB logs use a **space-separated format** with **29 fields**. Quoted fields handle spaces within values.

### Example Log Entry

```
https 2024-01-15T12:30:45.123456Z app/my-alb/abc123 192.0.2.100:54321 10.0.0.1:80 0.001 0.002 0.000 200 200 256 1024 "GET https://example.com/api/data?key=value HTTP/1.1" "Mozilla/5.0 (compatible; GPTBot/1.0)" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-tg/abc123 "Root=1-abc123" "example.com" "arn:aws:acm:us-east-1:123456789012:certificate/abc123" 0 2024-01-15T12:30:45.123456Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-"
```

### Parsed Result

```json
{
  "timestamp": "2024-01-15T12:30:45.123456+00:00",
  "client_ip": "192.0.2.100",
  "method": "GET",
  "host": "example.com",
  "path": "/api/data",
  "query_string": "key=value",
  "status_code": 200,
  "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
  "request_bytes": 256,
  "response_bytes": 1024,
  "response_time_ms": 3,
  "ssl_protocol": "TLSv1.2",
  "protocol": "HTTP/1.1",
  "extra": {
    "type": "https",
    "elb": "app/my-alb/abc123",
    "target_group_arn": "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-tg/abc123",
    "trace_id": "Root=1-abc123"
  }
}
```

---

## Usage Examples

### Basic Ingestion

```bash
# Ingest all logs from directory
python scripts/ingest_logs.py \
  --provider aws_alb \
  --input alb-logs/
```

### With Time Filtering

```bash
# Ingest logs from specific date range
python scripts/ingest_logs.py \
  --provider aws_alb \
  --input alb-logs/ \
  --start-time 2024-01-01 \
  --end-time 2024-01-31
```

### Filter LLM Bot Traffic

```bash
# Only ingest LLM bot requests
python scripts/ingest_logs.py \
  --provider aws_alb \
  --input alb-logs/ \
  --filter-bots
```

### Validate Before Ingestion

```bash
# Dry run to validate format
python scripts/ingest_logs.py \
  --provider aws_alb \
  --input alb-logs/ \
  --dry-run
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Invalid source_type" | Wrong source type | Use `--source-type alb_log_file` |
| Empty records | Malformed request lines | Adapter skips `"- - -"` requests |
| Missing client_ip | IPv6 parsing issue | Check for bracket notation |
| Negative processing times | No backend connection | Normal for 502/503 errors |

### Malformed Request Lines

ALB logs may contain entries with `"- - -"` for the request field when the request was malformed. These are automatically skipped:

```
http 2024-01-15T12:30:48.000000Z app/my-alb/abc123 192.0.2.103:34567 10.0.0.3:80 0.001 0.001 0.000 400 400 128 256 "- - -" "-" - - ...
```

### IPv6 Addresses

IPv6 addresses use bracket notation:
```
[2001:db8::1]:54321 → client_ip: "2001:db8::1"
```

### Error Log Entries

Entries with `-1` processing times indicate no backend connection:
```
https 2024-01-15T12:30:47Z app/my-alb/abc123 192.0.2.102:23456 - -1 -1 -1 502 - - - ...
```

These are still parsed, with `response_time_ms` set to `None`.

---

## Performance

| Metric | Value |
|--------|-------|
| Throughput | ~10,000 records/second |
| Memory | Streaming (constant memory usage) |
| Gzip support | Automatic decompression |

The ALB adapter uses `shlex.split()` for robust handling of quoted fields, which is slightly slower than simpler formats but handles all edge cases correctly.

---

## See Also

- [AWS ALB Access Logs Documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html)
- [CLI Usage Guide](../cli-usage.md)
- [Other Providers](other-providers.md)
