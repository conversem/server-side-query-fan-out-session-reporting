# Akamai DataStream 2 Test Fixtures

This directory contains sample log files in Akamai DataStream 2 format for testing the `AkamaiAdapter`.

## Files

### `sample.json`
Standard JSON array with Akamai CamelCase field names:
- 3 records with LLM bot user agents (GPTBot, ClaudeBot, ChatGPT-User)
- ISO 8601 timestamps
- All common fields populated (bytes, turnaroundTimeMs, cacheStatus, tlsVersion)

### `sample.ndjson`
NDJSON (newline-delimited JSON) with same content as sample.json.

### `sample.json.gz`
Gzip-compressed version of sample.json for testing compression support.

### `edge_cases.json`
Special cases for testing robust parsing:
1. **Unix epoch seconds** - `reqTimeSec` with 10-digit timestamp
2. **Unix epoch milliseconds** - `reqTimeMs` with 13-digit timestamp
3. **IPv6 client IP** - `2001:db8::1`
4. **Null optional fields** - host and bytes are null

## Field Mapping Reference

| Akamai Field | Universal Field |
|--------------|-----------------|
| requestTime | timestamp |
| clientIP | client_ip |
| requestMethod | method |
| requestHost | host |
| requestPath | path |
| responseStatus | status_code |
| userAgent | user_agent |
| bytes | response_bytes |
| turnaroundTimeMs | response_time_ms |
| queryString | query_string |
| tlsVersion | ssl_protocol |
| cacheStatus | cache_status |

## Expected Parsing Behavior

| Fixture | Records Expected | Notes |
|---------|------------------|-------|
| sample.json | 3 (with filter_bots=True) | All have LLM bot user agents |
| sample.ndjson | 3 (with filter_bots=True) | Same as sample.json |
| sample.json.gz | 3 (with filter_bots=True) | Same as sample.json |
| edge_cases.json | 4 (with filter_bots=True) | All have LLM bot user agents |

