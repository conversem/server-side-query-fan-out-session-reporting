# Greenfield CloudFront + ALB example

Reference composition of the four modules. Copy this directory into the
client's own Terraform repository and customise `variables.tf` defaults or
pass a `.tfvars`.

For an existing ALB and no CloudFront, leave `cloudfront_distribution_ids`
empty, set `alb_arns` / `alb_prefix_per_alb`, and enable `access_logs` on the
ALB you already own. Do not apply `greenfield-alb-only` in that case — that
example creates a new load balancer.

Set `client_name` and `log_bucket_name` in tfvars. Override `reader_role_name`
if you do not want the default `aws-logs-reader-<client_name>`.

## Deploy

```bash
cd infrastructure/terraform/aws/examples/greenfield-cf-and-alb
terraform init
terraform plan -var-file=prod.tfvars
terraform apply -var-file=prod.tfvars
```

## Outputs you'll need on the GCP side

| Output | Used by |
|---|---|
| `log_bucket_name` | `scripts/infrastructure/setup_aws_sts.py` (S3 bucket name) |
| `reader_role_arn` | `scripts/infrastructure/setup_aws_sts.py` (`awsS3DataSource.roleArn`) |

Pass them into `config.enc.yaml` under the per-domain `aws:` block (see
[`/docs/aws/onboarding-runbook.md`](../../../../docs/aws/onboarding-runbook.md)).

## After apply

1. Configure CloudFront standard logging v2 delivery for each distribution (see
   `modules/cloudfront-logging/README.md`).
2. Add `access_logs` to each ALB (see `modules/alb-logging/README.md`).
