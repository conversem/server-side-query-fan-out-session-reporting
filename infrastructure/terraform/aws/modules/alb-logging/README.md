# alb-logging

Builds the bucket-policy document that allows Application Load Balancer access
log delivery to write into the shared log bucket. This module is intentionally
policy-only so it can be merged into the single `aws_s3_bucket_policy` for the
bucket.

The ALB itself must still have access logging enabled. If the client owns the
ALB in Terraform, add an `access_logs` block to the `aws_lb` resource. If the
ALB is brownfield and not Terraform-managed yet, use the Console or AWS CLI as
a temporary fallback and reconcile the setting into Terraform later.

The policy covers **both** delivery paths AWS uses:

1. Per-region ELB service account (`data.aws_elb_service_account`) — the
   legacy path, still in use in most regions.
2. Service principal `logdelivery.elasticloadbalancing.amazonaws.com` — the
   new unified path.

## Usage

```hcl
module "alb_logging" {
  source         = "../../modules/alb-logging"
  bucket_arn     = module.log_bucket.bucket_arn
  alb_arns       = [aws_lb.api.arn]
  prefix_per_alb = { "api-prod" = "alb/api-prod" }
}
```

`alb_arns` scopes the service-principal policy with `aws:SourceArn`; the
prefix map scopes the S3 object paths under `AWSLogs/<account-id>/`.

If the client manages the ALB in Terraform, add:

```hcl
resource "aws_lb" "api" {
  # ... existing config ...

  access_logs {
    bucket  = module.log_bucket.bucket_name
    prefix  = "alb/api-prod"
    enabled = true
  }
}
```

If the ALB is not Terraform-managed yet, enable the same settings temporarily:

```bash
aws elbv2 modify-load-balancer-attributes \
  --load-balancer-arn "$ALB_ARN" \
  --attributes \
    Key=access_logs.s3.enabled,Value=true \
    Key=access_logs.s3.bucket,Value="$LOG_BUCKET_NAME" \
    Key=access_logs.s3.prefix,Value="$ALB_LOG_PREFIX"
```

Delivered objects appear under
`s3://<bucket>/alb/api-prod/AWSLogs/<account>/elasticloadbalancing/<region>/<yyyy>/<mm>/<dd>/<filename>.log.gz`.

See also `examples/greenfield-alb-only/` for a full ALB-as-entrypoint composition.
