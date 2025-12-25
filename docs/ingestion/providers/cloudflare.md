# Cloudflare Provider Guide

## Overview

Cloudflare supports two ingestion methods:

1. **Logpull API** - Real-time API access (7-day retention)
2. **Logpush Files** - CSV/JSON file exports (unlimited retention)

Both methods are supported by the Cloudflare adapter with automatic field mapping to the universal schema.

## Method 1: Logpull API

### Overview

Logpull API provides real-time access to Cloudflare logs via HTTP API. This method is best for:
- Recent data (last 7 days)
- Real-time ingestion
- Small to medium data volumes

### Prerequisites

1. **Cloudflare API Token**:
   - Go to Cloudflare Dashboard → My Profile → API Tokens
   - Create token with "Zone Logs:Read" permission
   - Save token securely

2. **Zone ID**:
   - Go to Cloudflare Dashboard → Select your domain
   - Zone ID is shown in the right sidebar

3. **Configuration**:

   **Option A: Environment Variables**:
   ```bash
   export CLOUDFLARE_API_TOKEN="your-api-token"
   export CLOUDFLARE_ZONE_ID="your-zone-id"
   ```

   **Option B: SOPS Encrypted Config** (recommended):
   ```yaml
   # config.enc.yaml
   cloudflare_api_token: "your-api-token"
   cloudflare_zone_id: "your-zone-id"
   ```

### Usage

```bash
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input api://zone_id \
  --start-date 2024-01-15T00:00:00 \
  --end-date 2024-01-16T00:00:00
```

**Note**: `start-date` and `end-date` are **required** for API sources. Bot filtering is **enabled by default**.

### API Limitations

- **Retention**: 7 days maximum
- **Rate Limits**: Subject to Cloudflare API rate limits
- **Time Range**: Must specify start and end times
- **Data Volume**: Best for small to medium volumes

## Method 2: Logpush Files

### Overview

Logpush exports Cloudflare logs to external storage (S3, GCS, Azure Blob) in CSV or JSON format. This method is best for:
- Historical data (unlimited retention)
- Large data volumes
- Batch processing

### Prerequisites

1. **Cloudflare Plan**: Enterprise, Business, or Pro plan (Logpush requirements vary)
2. **External Storage**: S3, GCS, Azure Blob, or other supported destination
3. **Storage Permissions**: Write access to the destination bucket

### Setting Up Logpush

#### Step 1: Create a Logpush Job

1. **Navigate to Logpush**:
   - Go to [Cloudflare Dashboard](https://dash.cloudflare.com/)
   - Select your zone
   - Go to **Analytics & Logs** → **Logpush**

2. **Create New Job**:
   - Click "Add a job" or "Create a Logpush job"
   - Select log type: **HTTP requests**
   
3. **Configure Destination**:
   
   **For Amazon S3**:
   ```
   Bucket name: your-bucket-name
   Bucket path: cloudflare-logs/
   Bucket region: us-east-1
   Access key ID: AKIAIOSFODNN7EXAMPLE
   Secret access key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
   ```

   **For Google Cloud Storage**:
   ```
   Bucket name: your-bucket-name
   Bucket path: cloudflare-logs/
   Service account credentials: (paste JSON key)
   ```

   **For Azure Blob Storage**:
   ```
   Container name: cloudflare-logs
   Storage account name: yourstorageaccount
   SAS token or shared key: (your credentials)
   ```

4. **Select Fields to Export**:
   
   For LLM bot analysis, include at minimum:
   - `EdgeStartTimestamp`
   - `ClientIP`
   - `ClientRequestMethod`
   - `ClientRequestHost`
   - `ClientRequestURI`
   - `EdgeResponseStatus`
   - `ClientRequestUserAgent`
   
   Recommended additional fields:
   - `EdgeResponseBytes`
   - `ClientRequestBytes`
   - `OriginResponseTime`
   - `CacheCacheStatus`
   - `EdgeColoCode`
   - `ClientRequestReferer`
   - `ClientRequestProtocol`

5. **Select Output Format**:
   - **JSON** (recommended): Easier to parse, includes field names
   - **CSV**: Smaller file size, header in first line
   - **NDJSON**: Best for streaming/large files

6. **Enable Job**:
   - Click "Save and activate"
   - Logs will start flowing within minutes

#### Step 2: Download Log Files

```bash
# From S3
aws s3 sync s3://your-log-bucket/cloudflare-logs/ ./cloudflare-logs/

# From GCS
gsutil -m rsync -r gs://your-log-bucket/cloudflare-logs/ ./cloudflare-logs/

# From Azure Blob
az storage blob download-batch \
  --account-name yourstorageaccount \
  --source cloudflare-logs \
  --destination ./cloudflare-logs/
```

#### Step 3: Verify Log Files

Check that files were downloaded:

```bash
ls -la cloudflare-logs/

# Sample output:
# -rw-r--r-- 1 user user 15234567 Jan 15 12:00 20240115T120000Z_20240115T130000Z.json.gz
# -rw-r--r-- 1 user user 14523456 Jan 15 13:00 20240115T130000Z_20240115T140000Z.json.gz
```

### Supported Formats

- **CSV** (`.csv`) - Comma-separated values
- **JSON** (`.json`) - Single JSON object or array
- **NDJSON** (`.ndjson`, `.jsonl`) - Newline-delimited JSON
- **Gzip compressed** (`.gz`) - Automatically decompressed

### Usage

**CSV File**:
```bash
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input cloudflare-logs/logpush.csv
```

**JSON File**:
```bash
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input cloudflare-logs/logpush.json
```

**NDJSON File**:
```bash
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input cloudflare-logs/logpush.ndjson
```

**Directory**:
```bash
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input cloudflare-logs/
```

### With Time Filtering

```bash
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input cloudflare-logs/ \
  --start-date 2024-01-15T00:00:00 \
  --end-date 2024-01-16T00:00:00
```

### With Bot Filtering

Bot filtering is **enabled by default**. To disable it and ingest all traffic:

```bash
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input cloudflare-logs/ \
  --no-filter-bots
```

## Field Mapping

The adapter automatically maps Cloudflare fields to the universal schema:

| Cloudflare Field | Universal Schema Field | Notes |
|-----------------|------------------------|-------|
| `EdgeStartTimestamp` | `timestamp` | Nanoseconds → UTC datetime |
| `ClientIP` | `client_ip` | Client IP address |
| `ClientRequestMethod` | `method` | HTTP method |
| `ClientRequestHost` | `host` | Host header |
| `ClientRequestURI` (path) | `path` | URI path |
| `ClientRequestURI` (query) | `query_string` | Query parameters |
| `EdgeResponseStatus` | `status_code` | HTTP status code |
| `ClientRequestUserAgent` | `user_agent` | User-Agent header |
| `EdgeResponseBytes` | `response_bytes` | Response size in bytes |
| `ClientRequestBytes` | `request_bytes` | Request size in bytes |
| `OriginResponseTime` | `response_time_ms` | Response time in milliseconds |
| `CacheCacheStatus` | `cache_status` | Cache status |
| `EdgeColoCode` | `edge_location` | Edge POP code |
| `ClientRequestReferer` | `referer` | Referer header |
| `ClientRequestProtocol` | `protocol` | HTTP protocol version |

## Choosing Between Methods

### Use Logpull API When:
- ✅ You need recent data (last 7 days)
- ✅ You want real-time ingestion
- ✅ Data volume is small to medium
- ✅ You don't want to manage file storage

### Use Logpush Files When:
- ✅ You need historical data (beyond 7 days)
- ✅ You have large data volumes
- ✅ You want to process data in batches
- ✅ You already have Logpush configured

## Example Workflows

### Real-Time Ingestion (API)

```bash
# Ingest last 24 hours
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input api://zone_id \
  --start-date $(date -u -d '1 day ago' +%Y-%m-%dT%H:%M:%S) \
  --end-date $(date -u +%Y-%m-%dT%H:%M:%S)
# Bot filtering is enabled by default
```

### Batch Processing (Files)

```bash
# Download logs from S3
aws s3 sync s3://my-cloudflare-logs/ ./cloudflare-logs/

# Ingest all files
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input cloudflare-logs/ \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
# Bot filtering is enabled by default
```

## Troubleshooting

### API Authentication Errors

**Error**: `Invalid API token` or `Unauthorized`

**Solution**:
- Verify API token has "Zone Logs:Read" permission
- Check token is correctly set in environment or config
- Ensure zone ID is correct

### Missing Start/End Date

**Error**: `start_time and end_time are required for API sources`

**Solution**: Always provide `--start-date` and `--end-date` for API sources.

### Retention Limit Exceeded

**Error**: `Requested time range exceeds 7-day retention limit`

**Solution**: Use Logpush files for data older than 7 days.

### URI Parsing Errors

**Error**: `Could not parse URI from ClientRequestURI`

**Solution**: The adapter handles URI parsing automatically. If you see this error, check the log file format matches Cloudflare Logpush format.

### Timestamp Conversion Errors

**Error**: `Could not convert EdgeStartTimestamp to datetime`

**Solution**: Cloudflare timestamps are in nanoseconds. The adapter handles conversion automatically. If you see this error, the log file may be corrupted.

See [Troubleshooting Guide](../troubleshooting.md) for more solutions.

## Performance Tips

- **Use Logpush for large volumes**: File-based ingestion is more efficient for large datasets
- **Filter early**: Use `--start-date` and `--end-date` to reduce processing time
- **Batch API calls**: The adapter batches API requests automatically
- **Use gzip compression**: Compressed files are faster to process

See [Performance Tuning Guide](../performance-tuning.md) for optimization recommendations.

