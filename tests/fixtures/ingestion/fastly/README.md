# Fastly Log Test Fixtures

This directory contains sample log files in Fastly's configurable log formats for testing the `FastlyAdapter`.

## Files

### `sample.json`
Standard JSON array with default field names:
- 3 records with LLM bot user agents (GPTBot, ClaudeBot, ChatGPT-User)
- ISO 8601 timestamps
- All standard fields populated

### `sample.ndjson`
NDJSON (newline-delimited JSON) with same content as sample.json.

### `sample.csv`
CSV format with header row:
- Same 3 records as sample.json
- Demonstrates quoted fields for user agents

### `custom_fields.json`
JSON with alternative field names (common variations):
- `request_time` instead of `timestamp`
- `clientip` instead of `client_ip`
- `http_method` instead of `method`
- `hostname` instead of `host`
- `uri` instead of `path`
- `http_status` instead of `status_code`
- `ua` instead of `user_agent`
- `bytes` instead of `response_bytes`

### `edge_cases.json`
Special cases for testing robust parsing:
1. **Unix timestamp** - Integer timestamp instead of ISO 8601
2. **IPv6 client IP** - `2001:db8::1`
3. **Missing optional fields** - No query_string, response_bytes, etc.
4. **Null values** - host and optional fields are null

## Field Mapping Reference

| Default Field | Common Alternatives |
|---------------|---------------------|
| timestamp | time, date, request_time, start_time |
| client_ip | clientip, client, ip, remote_addr |
| method | http_method, request_method, verb |
| host | hostname, server_name, domain |
| path | uri, url, request_uri, request_path |
| status_code | status, http_status, response_code |
| user_agent | useragent, user-agent, ua |
| response_bytes | bytes, body_bytes, size, bytes_sent |

## Expected Parsing Behavior

| Fixture | Records Expected | Notes |
|---------|------------------|-------|
| sample.json | 3 (with filter_bots=True) | All have LLM bot user agents |
| sample.ndjson | 3 (with filter_bots=True) | Same as sample.json |
| sample.csv | 3 (with filter_bots=True) | Same as sample.json |
| custom_fields.json | 2 (with filter_bots=True) | Uses alias field names |
| edge_cases.json | 4 (with filter_bots=True) | All have LLM bot user agents |

