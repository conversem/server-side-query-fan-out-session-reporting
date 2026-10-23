# AWS infrastructure for the LLM Bot Pipeline

Terraform modules the client applies in their own AWS account to expose
CloudFront and ALB access logs to the GCP pipeline.

## Layout

```
infrastructure/terraform/aws/
├── modules/
│   ├── log-bucket/         S3 bucket + lifecycle + encryption
│   ├── reader-role/        IAM role with GCP-OIDC trust
│   ├── cloudfront-logging/ Policy document for CloudFront log delivery
│   └── alb-logging/        Policy document for ALB log delivery
└── examples/
    ├── greenfield-cf-and-alb/  Reference composition for mixed or optional sources
    └── greenfield-alb-only/    Reference composition with aws_lb.access_logs enabled
```

## Onboarding flow

1. **GCP side first**: have the Storage Transfer Service service agent's
   numeric `subjectId` to hand. Retrieve it with
   `storagetransfer.projects.getServiceAccount` / `googleServiceAccounts.get`
   for the GCP project that owns the transfer job.
2. Choose the reference composition:
   - `examples/greenfield-cf-and-alb/` when CloudFront is present, sources are mixed, **or the ALB already exists** (leave the CloudFront lists empty).
   - `examples/greenfield-alb-only/` only when you need this stack to **create** a new ALB.
3. Copy the chosen example into the client's own Terraform repository (do not commit client deployments into this repo).
4. Edit variables or write a `.tfvars` for the client. Set `client_name`, `log_bucket_name`, and optionally `reader_role_name` (defaults to `aws-logs-reader-<client_name>`). These are Terraform variables, not environment variables.
5. `terraform init && terraform plan && terraform apply`.
6. Capture outputs `log_bucket_name` and `reader_role_arn`.
7. For CloudFront clients: configure standard logging v2 delivery for each
   distribution (see `modules/cloudfront-logging/README.md`).
8. For ALB clients: ensure `access_logs` is enabled on each `aws_lb` (see `modules/alb-logging/README.md` and `examples/greenfield-alb-only/`).
9. Hand off `log_bucket_name` and `reader_role_arn` to the GCP-side onboarding (`docs/aws/onboarding-runbook.md`).

For ALB-only clients, the critical Terraform setting is the `access_logs` block
on the client-owned `aws_lb`. The `alb-logging` module only grants S3 write
permission; it does not mutate a production load balancer by ARN.

## Security model

- **No long-lived credentials**: GCP Storage Transfer Service assumes the AWS
  reader role as its Google-managed service agent via OIDC federated identity
  (Google as the IdP).
- **Tight trust**: the trust policy is scoped to a single
  `accounts.google.com:sub` — the Storage Transfer Service service agent's
  numeric `subjectId`.
- **Tight policy**: the reader role can only `s3:ListBucket`, `s3:GetObject`,
  `s3:GetObjectAttributes`, `s3:GetBucketLocation`. No writes. No bucket
  policy mutations.
- **Encryption**: SSE-S3 (AES256). See `modules/log-bucket/README.md` for
  the SSE-KMS trade-off.
- **Lifecycle**: logs transition to STANDARD_IA after 30 days and expire
  after 90 (both configurable).

## Multi-source bucket policy (D4)

Only one `aws_s3_bucket_policy` may exist per bucket. The
`cloudfront-logging` and `alb-logging` modules emit **policy documents**
(`policy_json` outputs). The example composition merges them into a single
bucket policy via `data.aws_iam_policy_document.bucket`.
