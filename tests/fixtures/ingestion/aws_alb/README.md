# AWS ALB Access Log Test Fixtures

This directory contains sample log files in AWS ALB access log format for testing the `ALBAdapter`.

## Files

### `sample.log`
Standard ALB access log with 3 entries featuring:
- ISO 8601 timestamps with microsecond precision
- Full request lines with different HTTP methods (GET, POST)
- LLM bot user agents (GPTBot, ClaudeBot, ChatGPT-User)
- SSL/TLS details

### `sample.log.gz`
Gzip-compressed version of sample.log for testing compression support.

### `edge_cases.log`
Special cases for testing robust parsing:
1. **Standard request** - Complete ALB log entry
2. **Backend timeout** - 502 error with `-1` processing times and missing target
3. **Malformed request line** - `"- - -"` request (should be skipped)
4. **IPv6 client IP** - Bracketed IPv6 format `[2001:db8::1]:port`
5. **Relative URL** - Request without scheme/host in URL

## ALB Log Format (Space-Separated)

Fields are space-separated, with some fields quoted:

```
type time elb client:port target:port request_processing_time target_processing_time response_processing_time elb_status_code target_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol target_group_arn "trace_id" "domain_name" "chosen_cert_arn" matched_rule_priority request_creation_time "actions_executed" "redirect_url" "error_reason" "target:port_list" "target_status_code_list" "classification" "classification_reason"
```

## Expected Parsing Behavior

| Fixture | Records Expected | Notes |
|---------|------------------|-------|
| sample.log | 3 (with filter_bots=True) | All have LLM bot user agents |
| sample.log.gz | 3 (with filter_bots=True) | Same as sample.log |
| edge_cases.log | 4 (with filter_bots=False) | Entry 3 skipped (malformed request) |

## Field Mapping Reference

| ALB Position | Field | Universal Field |
|--------------|-------|-----------------|
| 2 | time | timestamp |
| 4 | client:port | client_ip (extract IP) |
| 9 | elb_status_code | status_code |
| 13 | "request" | method, host, path, query_string |
| 14 | "user_agent" | user_agent |
| 11 | received_bytes | request_bytes |
| 12 | sent_bytes | response_bytes |
| 16 | ssl_protocol | ssl_protocol |
| 6+7+8 | processing times | response_time_ms (sum × 1000) |

