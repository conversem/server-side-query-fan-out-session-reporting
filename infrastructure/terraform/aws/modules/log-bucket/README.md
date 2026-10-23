# log-bucket

Creates the S3 bucket used to receive CloudFront and ALB access logs.

## Design choices

- **Encryption**: SSE-S3 (AES256). SSE-KMS is not used because Cloudflare/ALB
  log delivery and GCP Storage Transfer Service work most reliably with
  unencrypted-key-managed objects. Revisit if a client mandates KMS.
- **Lifecycle**: transition to `STANDARD_IA` after 30 days, expire after 90.
  Both knobs are configurable via variables.
- **Ownership**: `BucketOwnerEnforced` so ALB log delivery (which historically
  required ACLs) uses the modern owner-only path via bucket policy.
- **Public access**: fully blocked.

## Usage

```hcl
module "log_bucket" {
  source             = "../../modules/log-bucket"
  bucket_name        = "aws-logs-acme-prod"
  log_retention_days = 90
  tags = { project = "llm-bot-pipeline", client = "acme" }
}
```

## Outputs

| Name | Purpose |
|---|---|
| `bucket_name` | Pass into other modules (`reader-role`, `cloudfront-logging`, `alb-logging`). |
| `bucket_arn` | Pass into `reader-role` for the read-policy. |
| `bucket_regional_domain_name` | Bucket regional domain name; not used by the standard logging v2 path. |
