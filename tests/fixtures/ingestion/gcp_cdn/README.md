# GCP Cloud CDN Test Fixtures

This directory contains sample log files in GCP Cloud Logging format for testing the `GCPCDNAdapter`.

## Files

### `sample.json`
Standard JSON array with 3 complete log entries featuring:
- RFC3339 timestamps with microsecond precision
- Full httpRequest objects with all common fields
- LLM bot user agents (GPTBot, ClaudeBot, ChatGPT-User)
- Cache hits and misses
- Various HTTP methods

### `sample.ndjson`
NDJSON (newline-delimited JSON) format with 3 entries, same content as sample.json.

### `sample.json.gz`
Gzip-compressed version of sample.json for testing compression support.

### `edge_cases.json`
Special cases for testing robust parsing:
1. **Minimal required fields** - Entry with only required httpRequest fields
2. **Relative URL** - requestUrl without scheme/host (e.g., `/relative/path`)
3. **IPv6 client IP** - remoteIp with IPv6 address, plus cache bypass
4. **Missing httpRequest fields** - Entry with only status code (should be skipped)
5. **Missing httpRequest entirely** - Entry without httpRequest (should be skipped)
6. **Timezone offset** - Timestamp with explicit timezone offset (-05:00)
7. **Resource labels** - Entry with resource.labels for zone extraction

## Expected Parsing Behavior

| Fixture | Records Expected | Notes |
|---------|------------------|-------|
| sample.json | 3 (with filter_bots=True) | All have LLM bot user agents |
| sample.ndjson | 3 (with filter_bots=True) | Same as sample.json |
| edge_cases.json | 4 (with filter_bots=True) | Entries 4 and 5 should be skipped due to missing required fields |

## Field Mapping Reference

| GCP Field | Universal Field |
|-----------|-----------------|
| timestamp | timestamp |
| httpRequest.remoteIp | client_ip |
| httpRequest.requestMethod | method |
| httpRequest.requestUrl | host + path + query_string |
| httpRequest.status | status_code |
| httpRequest.userAgent | user_agent |
| httpRequest.requestSize | request_bytes |
| httpRequest.responseSize | response_bytes |
| httpRequest.latency | response_time_ms |
| httpRequest.cacheHit | cache_status |
| httpRequest.referer | referer |
| httpRequest.protocol | protocol |
| httpRequest.serverIp | edge_location |

