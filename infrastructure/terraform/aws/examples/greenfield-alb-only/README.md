# Greenfield ALB-only example

Reference composition for clients where the AWS entrypoint is an Application
Load Balancer and there is no CloudFront distribution in front of it.

This example creates:

- S3 log bucket
- GCP OIDC reader role
- bucket policy for ALB log delivery
- example `aws_lb` with `access_logs` enabled

## Important split

`modules/alb-logging` grants S3 write permission for ALB log delivery. The ALB
itself is enabled with this block on the client-owned `aws_lb` resource:

```hcl
access_logs {
  bucket  = module.log_bucket.bucket_name
  prefix  = var.alb_log_prefix
  enabled = true
}
```

If the client already has an ALB, do **not** apply this example as-is. Copy
the `access_logs` block into their existing `aws_lb` and compose
`log-bucket` + `reader-role` + `alb-logging` instead (see
`examples/greenfield-cf-and-alb/` with empty CloudFront lists).

Set `client_name` and `log_bucket_name` in tfvars. Override `reader_role_name`
if you do not want the default `aws-logs-reader-<client_name>`.

## Deploy

```bash
cd infrastructure/terraform/aws/examples/greenfield-alb-only
terraform init
terraform plan -var-file=prod.tfvars
terraform apply -var-file=prod.tfvars
```

## Outputs needed on the GCP side

| Output | Used by |
|---|---|
| `log_bucket_name` | `scripts/infrastructure/setup_aws_sts.py` S3 source bucket |
| `reader_role_arn` | `scripts/infrastructure/setup_aws_sts.py` `awsS3DataSource.roleArn` |
| `alb_log_s3_prefix` | Per-domain `aws.s3_prefixes[]` reference |

## Verify ALB delivery

After apply, AWS validates bucket permissions and writes a test file:

```text
s3://<bucket>/<prefix>/AWSLogs/<account-id>/ELBAccessLogTestFile
```

Real access logs arrive under:

```text
s3://<bucket>/<prefix>/AWSLogs/<account-id>/elasticloadbalancing/<region>/<yyyy>/<mm>/<dd>/...
```

## Brownfield fallback

If the client's ALB is not Terraform-managed yet, enable the same attributes
temporarily with the AWS Console or CLI:

```bash
aws elbv2 modify-load-balancer-attributes \
  --load-balancer-arn "$ALB_ARN" \
  --attributes \
    Key=access_logs.s3.enabled,Value=true \
    Key=access_logs.s3.bucket,Value="$LOG_BUCKET_NAME" \
    Key=access_logs.s3.prefix,Value="$ALB_LOG_PREFIX"
```

Reconcile this into Terraform later to avoid long-term drift.
