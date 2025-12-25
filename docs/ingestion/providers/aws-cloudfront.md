# AWS CloudFront Provider Guide

## Overview

AWS CloudFront logs are exported in **W3C Extended Log Format** with tab-separated values. The CloudFront adapter automatically handles field mapping from CloudFront's native format to the universal schema.

## Prerequisites

Before ingesting CloudFront logs, ensure you have:

1. **AWS Account** with CloudFront distribution
2. **S3 Bucket** configured for log storage
3. **Standard Logging** enabled on your CloudFront distribution
4. **AWS CLI** installed (for downloading logs)
5. **Sufficient disk space** for downloaded log files

## Supported Formats

- **W3C Extended Log Format** (`.log`, `.txt`) - Tab-separated values
- **Gzip compressed** (`.log.gz`, `.txt.gz`) - Automatically decompressed

## Exporting CloudFront Logs

### Step 1: Enable CloudFront Logging

CloudFront automatically exports logs to an S3 bucket you configure:

1. **Navigate to CloudFront Console**:
   - Go to [AWS Console](https://console.aws.amazon.com/) → CloudFront → Distributions
   - Select your distribution

2. **Enable Standard Logging**:
   - Go to "General" tab → "Edit"
   - Under "Standard logging":
     - Set **Logging** to "On"
     - Select or create an **S3 bucket** for logs
     - Optionally set a **Log prefix** (e.g., `cloudfront-logs/`)
   - Click "Save changes"

3. **Configure S3 Bucket Permissions** (if new bucket):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "AllowCloudFrontLogs",
         "Effect": "Allow",
         "Principal": {
           "Service": "cloudfront.amazonaws.com"
         },
         "Action": "s3:PutObject",
         "Resource": "arn:aws:s3:::your-log-bucket/*"
       }
     ]
   }
   ```

4. **Wait for Log Delivery**:
   - CloudFront delivers logs to S3 within 24 hours
   - Logs are delivered in hourly files (approximately)

### Step 2: Download Logs from S3

```bash
# Install AWS CLI if not already installed
pip install awscli

# Configure AWS credentials
aws configure

# Download all logs
aws s3 sync s3://your-log-bucket/cloudfront-logs/ ./cloudfront-logs/

# Or download specific date range (by prefix)
aws s3 sync s3://your-log-bucket/cloudfront-logs/ ./cloudfront-logs/ \
  --exclude "*" \
  --include "*2024-01-15*"
```

### Step 3: Verify Log Files

Log files follow this naming convention:

```
cloudfront-logs/
├── E1234567890ABC.2024-01-15-12.abcd1234.log.gz
├── E1234567890ABC.2024-01-15-13.efgh5678.log.gz
└── ...
```

Where:
- `E1234567890ABC` - Distribution ID
- `2024-01-15-12` - Date and hour (UTC)
- `abcd1234` - Random unique ID

### Log File Format

CloudFront logs use W3C Extended Log Format with these directives:

```
#Version: 1.0
#Fields: date time c-ip cs-method cs(Host) cs-uri-stem cs-uri-query sc-status cs(User-Agent) sc-bytes cs-bytes time-taken x-edge-result-type x-edge-location cs(Referer) cs-protocol ssl-protocol
2024-01-15	12:30:45	192.0.2.100	GET	example.com	/api/data	key=value	200	Mozilla/5.0%20(compatible;%20GPTBot/1.0)	1024	256	0.150	Hit	LAX	https://example.com/referer	https	TLSv1.3
```

## Field Mapping

The adapter automatically maps CloudFront fields to the universal schema:

| CloudFront Field | Universal Schema Field | Notes |
|-----------------|------------------------|-------|
| `date` + `time` | `timestamp` | Combined into single UTC datetime |
| `c-ip` | `client_ip` | Client IP address |
| `cs-method` | `method` | HTTP method |
| `cs(Host)` | `host` | Host header |
| `cs-uri-stem` | `path` | URI path |
| `cs-uri-query` | `query_string` | Query parameters (URL-decoded) |
| `sc-status` | `status_code` | HTTP status code |
| `cs(User-Agent)` | `user_agent` | User-Agent header (URL-decoded) |
| `sc-bytes` | `response_bytes` | Response size in bytes |
| `cs-bytes` | `request_bytes` | Request size in bytes |
| `time-taken` | `response_time_ms` | Response time in seconds → milliseconds |
| `x-edge-result-type` | `cache_status` | Cache status (Hit/Miss) |
| `x-edge-location` | `edge_location` | Edge POP code |
| `cs(Referer)` | `referer` | Referer header |
| `cs-protocol` | `protocol` | HTTP protocol version |
| `ssl-protocol` | `ssl_protocol` | TLS version |

## Usage

### Single File

```bash
python scripts/ingest_logs.py \
  --provider aws_cloudfront \
  --input cloudfront-logs/E1234567890ABC.2024-01-15-12.abcd1234.log.gz
```

### Directory

Process all log files in a directory:

```bash
python scripts/ingest_logs.py \
  --provider aws_cloudfront \
  --input cloudfront-logs/
```

The adapter will:
- Recursively find all `.log`, `.txt`, `.log.gz`, `.txt.gz` files
- Process each file
- Skip non-log files

### With Time Filtering

Filter records by timestamp:

```bash
python scripts/ingest_logs.py \
  --provider aws_cloudfront \
  --input cloudfront-logs/ \
  --start-date 2024-01-15T12:00:00 \
  --end-date 2024-01-15T13:00:00
```

### With Bot Filtering

Bot filtering is **enabled by default**. To disable it and ingest all traffic:

```bash
python scripts/ingest_logs.py \
  --provider aws_cloudfront \
  --input cloudfront-logs/ \
  --no-filter-bots
```

### Auto-Detection

The system can auto-detect CloudFront format from file headers:

```bash
python scripts/ingest_logs.py --input cloudfront-logs/sample.log
```

## Features

- **Automatic gzip decompression**: Handles `.gz` files transparently
- **URL decoding**: Automatically decodes URL-encoded fields (User-Agent, query strings)
- **Timestamp construction**: Combines `date` and `time` fields into UTC datetime
- **Response time conversion**: Converts seconds to milliseconds
- **Directory processing**: Recursively processes directories
- **Time filtering**: Filter records by timestamp range
- **Bot filtering**: Filter to only LLM bot traffic

## Example Workflow

1. **Export logs from S3**:
   ```bash
   aws s3 sync s3://my-cloudfront-logs/ ./cloudfront-logs/
   ```

2. **Ingest logs**:
   ```bash
   python scripts/ingest_logs.py \
     --provider aws_cloudfront \
     --input cloudfront-logs/ \
     --start-date 2024-01-15 \
     --end-date 2024-01-16
   # Bot filtering is enabled by default
   ```

3. **Verify ingestion**:
   ```bash
   # Check database
   sqlite3 data/llm-bot-logs.db "SELECT COUNT(*) FROM raw_bot_requests;"
   ```

## Troubleshooting

### Missing #Fields Directive

**Error**: `Missing #Fields directive in W3C log file`

**Solution**: Ensure the log file starts with `#Version:` and `#Fields:` directives. CloudFront logs should include these automatically.

### Invalid Date/Time Format

**Error**: `Could not parse timestamp from date/time fields`

**Solution**: CloudFront logs use `YYYY-MM-DD` for date and `HH:MM:SS` for time. Ensure these fields are present in the `#Fields` directive.

### URL Encoding Issues

**Error**: `Invalid percent-encoding in field`

**Solution**: The adapter handles URL decoding automatically. If you see this error, the log file may be corrupted. Check the raw log file.

### Empty Directory

**Error**: `No matching files found in directory`

**Solution**: Ensure the directory contains `.log`, `.txt`, `.log.gz`, or `.txt.gz` files. Check file extensions match CloudFront log format.

See [Troubleshooting Guide](../troubleshooting.md) for more solutions.

## Performance Tips

- **Use gzip compression**: Compressed files are faster to transfer and process
- **Process directories**: The adapter efficiently processes multiple files
- **Filter early**: Use `--start-date` and `--end-date` to reduce processing time
- **Batch processing**: Large directories are processed file-by-file to manage memory

See [Performance Tuning Guide](../performance-tuning.md) for optimization recommendations.

