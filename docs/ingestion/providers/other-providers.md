# Ingesting Logs from CDN and Cloud Providers

This guide explains how to ingest logs from various CDN and cloud providers using **native adapters** that automatically handle field mapping and format parsing.

## Overview

The ingestion pipeline provides **native adapters** for direct ingestion without manual conversion:

| Provider | Adapter | Supported Formats |
|----------|---------|-------------------|
| [Azure CDN / Front Door](#azure-cdn--azure-front-door) | `azure_cdn` | JSON, NDJSON, CSV |
| [Google Cloud CDN](#google-cloud-cdn) | `gcp_cdn` | JSON, NDJSON |
| [AWS CloudFront](#aws-cloudfront) | `aws_cloudfront` | W3C Extended Log Format |
| [AWS ALB](#aws-application-load-balancer) | `aws_alb` | Space-separated logs |
| [Fastly](#fastly) | `fastly` | JSON, NDJSON, CSV |
| [Akamai DataStream](#akamai) | `akamai` | JSON, NDJSON |
| [Cloudflare](#cloudflare) | `cloudflare` | JSON, NDJSON, CSV, API |
| [Other providers](#universal-format) | `universal` | CSV, JSON, NDJSON |

---

## Azure CDN / Azure Front Door

### Exporting Logs

Azure CDN logs can be exported via **Azure Monitor** or **Log Analytics**.

#### Option 1: Export to Storage Account

1. **Navigate to Diagnostic Settings**:
   - Go to [Azure Portal](https://portal.azure.com/)
   - Select your CDN profile or Front Door
   - Go to **Monitoring** → **Diagnostic settings**

2. **Create Diagnostic Setting**:
   - Click "Add diagnostic setting"
   - Name: `cdn-logs-export`
   - Check **FrontDoorAccessLog** or **AzureCdnAccessLog**
   - Under **Destination details**, select "Archive to a storage account"
   - Select your storage account
   - Click "Save"

3. **Download Logs**:
   ```bash
   # Install Azure CLI
   pip install azure-cli
   
   # Login
   az login
   
   # Download logs
   az storage blob download-batch \
     --account-name yourstorageaccount \
     --source '$logs' \
     --destination ./azure-cdn-logs/
   ```

### Direct Ingestion

```bash
# Ingest Azure CDN/Front Door logs directly
python scripts/ingest_logs.py \
  --provider azure_cdn \
  --input azure-cdn-logs/

# Or from a single file
python scripts/ingest_logs.py \
  --provider azure_cdn \
  --input azure-cdn-logs/frontdoor-access.json
```

### Field Mapping

The Azure CDN adapter automatically maps these fields:

| Azure Field | Universal Field |
|-------------|-----------------|
| `TimeGenerated` / `time` | `timestamp` |
| `clientIp_s` / `clientIP` | `client_ip` |
| `requestMethod_s` / `httpMethod` | `method` |
| `hostName_s` / `host` | `host` |
| `requestUri_s` / `requestUri` | `path` |
| `httpStatusCode_d` / `httpStatusCode` | `status_code` |
| `userAgent_s` / `userAgent` | `user_agent` |

---

## Google Cloud CDN

### Exporting Logs

Google Cloud CDN logs are available through **Cloud Logging**.

#### Create Log Sink to Cloud Storage

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

3. **Download Logs**:
   ```bash
   gsutil -m rsync -r gs://your-log-bucket/cdn-logs/ ./gcp-cdn-logs/
   ```

### Direct Ingestion

```bash
# Ingest GCP Cloud CDN logs directly
python scripts/ingest_logs.py \
  --provider gcp_cdn \
  --input gcp-cdn-logs/

# With time filtering
python scripts/ingest_logs.py \
  --provider gcp_cdn \
  --input gcp-cdn-logs/ \
  --start-time 2024-01-01 \
  --end-time 2024-01-31
```

### Field Mapping

The GCP CDN adapter automatically extracts fields from nested `httpRequest` objects:

| GCP Field | Universal Field |
|-----------|-----------------|
| `timestamp` | `timestamp` |
| `httpRequest.remoteIp` | `client_ip` |
| `httpRequest.requestMethod` | `method` |
| `httpRequest.requestUrl` (parsed) | `host`, `path` |
| `httpRequest.status` | `status_code` |
| `httpRequest.userAgent` | `user_agent` |

---

## AWS CloudFront

### Exporting Logs

CloudFront logs can be configured to export to S3.

1. **Enable Standard Logging**:
   - Go to CloudFront console
   - Select your distribution
   - Enable **Standard logging** under "Standard logging" settings
   - Specify S3 bucket for log delivery

2. **Download Logs**:
   ```bash
   aws s3 sync s3://your-cloudfront-logs-bucket/ ./cloudfront-logs/
   ```

### Direct Ingestion

```bash
# Ingest CloudFront W3C logs directly
python scripts/ingest_logs.py \
  --provider aws_cloudfront \
  --input cloudfront-logs/

# From gzipped files (automatically detected)
python scripts/ingest_logs.py \
  --provider aws_cloudfront \
  --input cloudfront-logs/E2ABCD1234.2024-01-15-12.abcd1234.gz
```

See [AWS CloudFront Format](aws-cloudfront-format.md) for detailed documentation.

---

## AWS Application Load Balancer

### Exporting Logs

ALB logs are exported to S3 automatically when enabled.

1. **Enable Access Logging**:
   - Go to EC2 Console → Load Balancers
   - Select your ALB
   - Edit attributes → Enable "Access logs"
   - Specify S3 bucket

2. **Download Logs**:
   ```bash
   aws s3 sync s3://your-alb-logs-bucket/ ./alb-logs/
   ```

### Direct Ingestion

```bash
# Ingest ALB logs directly (space-separated format)
python scripts/ingest_logs.py \
  --provider aws_alb \
  --input alb-logs/

# From gzipped files
python scripts/ingest_logs.py \
  --provider aws_alb \
  --input alb-logs/123456789012_elasticloadbalancing_us-east-1_app.my-alb.log.gz
```

See [AWS ALB Format](aws-alb-format.md) for detailed field mapping.

---

## Fastly

### Exporting Logs

Fastly supports real-time log streaming to various destinations.

1. **Configure Log Streaming**:
   - Go to [Fastly Console](https://manage.fastly.com/)
   - Select your service → **Logging** → **Create endpoint**
   - Choose S3, GCS, or other destination
   - Recommended format: JSON or NDJSON

2. **Download Logs**:
   ```bash
   aws s3 sync s3://your-bucket/fastly-logs/ ./fastly-logs/
   ```

### Direct Ingestion

```bash
# Ingest Fastly JSON logs
python scripts/ingest_logs.py \
  --provider fastly \
  --input fastly-logs/

# Ingest Fastly NDJSON logs
python scripts/ingest_logs.py \
  --provider fastly \
  --source-type fastly_ndjson_file \
  --input fastly-logs/access.ndjson

# Ingest Fastly CSV logs
python scripts/ingest_logs.py \
  --provider fastly \
  --source-type fastly_csv_file \
  --input fastly-logs/access.csv
```

See [Fastly Format](fastly-format.md) for field mapping details.

---

## Akamai

### Exporting Logs

Akamai provides logs via **DataStream 2**.

1. **Configure DataStream 2**:
   - Go to Akamai Control Center
   - Navigate to **Delivery** → **DataStream**
   - Create new stream with S3 or GCS destination
   - Output Format: JSON or NDJSON

2. **Download Logs**:
   ```bash
   aws s3 sync s3://your-datastream-bucket/ ./akamai-logs/
   ```

### Direct Ingestion

```bash
# Ingest Akamai DataStream JSON logs
python scripts/ingest_logs.py \
  --provider akamai \
  --input akamai-logs/

# Ingest NDJSON format
python scripts/ingest_logs.py \
  --provider akamai \
  --source-type akamai_ndjson_file \
  --input akamai-logs/datastream.ndjson
```

See [Akamai Format](akamai-format.md) for camelCase field mapping.

---

## Cloudflare

### Exporting Logs

Cloudflare logs can be accessed via **Logpush** or the **API**.

1. **Configure Logpush**:
   - Go to Cloudflare Dashboard
   - Navigate to **Analytics** → **Logs**
   - Create Logpush job to S3, GCS, or R2

2. **Download Logs**:
   ```bash
   aws s3 sync s3://your-logpush-bucket/ ./cloudflare-logs/
   ```

### Direct Ingestion

```bash
# Ingest Cloudflare Logpush NDJSON
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input cloudflare-logs/

# Ingest from API (requires configuration)
python scripts/ingest_logs.py \
  --provider cloudflare \
  --source-type api \
  --input api://zone-id
```

See [Cloudflare Format](cloudflare-format.md) for complete documentation.

---

## Universal Format

For providers without native adapters, use the **Universal Format** adapter with properly formatted CSV, JSON, or NDJSON files.

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | ISO 8601 or Unix | Request time |
| `client_ip` | String | Client IP address |
| `method` | String | HTTP method |
| `host` | String | Request host |
| `path` | String | URI path |
| `status_code` | Integer | HTTP status |
| `user_agent` | String | User-Agent header |

### Example Format

**CSV**:
```csv
timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api/data,200,"Mozilla/5.0 (compatible; GPTBot/1.0)"
```

**JSON/NDJSON**:
```json
{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api/data", "status_code": 200, "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)"}
```

### Ingestion

```bash
python scripts/ingest_logs.py \
  --provider universal \
  --input your-logs.csv
```

See [Universal Format Specification](../universal-format.md) for complete schema.

---

## Auto-Detection

The CLI can automatically detect the provider from file content:

```bash
# Auto-detect provider
python scripts/ingest_logs.py --input logs/access.json

# This analyzes field patterns to identify:
# - Akamai: camelCase fields like clientIP, requestMethod
# - GCP: nested httpRequest objects
# - Azure: operationName, properties fields
# - Cloudflare: EdgeStartTimestamp, CacheCacheStatus
# - Fastly: cache_status with pop/datacenter
# - ALB: Space-separated logs starting with http/https
```

---

## Common Options

All providers support these options:

```bash
# Filter by time range
python scripts/ingest_logs.py \
  --provider <provider> \
  --input logs/ \
  --start-time 2024-01-01 \
  --end-time 2024-01-31

# Filter only LLM bot traffic
python scripts/ingest_logs.py \
  --provider <provider> \
  --input logs/ \
  --filter-bots

# Validate without inserting
python scripts/ingest_logs.py \
  --provider <provider> \
  --input logs/ \
  --validate-only

# Dry run (parse but don't insert)
python scripts/ingest_logs.py \
  --provider <provider> \
  --input logs/ \
  --dry-run
```

---

## See Also

- [CLI Usage Guide](../cli-usage.md)
- [Universal Format Specification](../universal-format.md)
- [Troubleshooting](../troubleshooting.md)
- Individual provider guides:
  - [AWS CloudFront](aws-cloudfront.md)
  - [AWS ALB](aws-alb-format.md)
  - [Fastly](fastly-format.md)
  - [Akamai](akamai-format.md)
  - [GCP Cloud CDN](gcp-cdn-format.md)
  - [Cloudflare](cloudflare.md)
