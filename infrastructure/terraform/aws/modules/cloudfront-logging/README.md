# cloudfront-logging

Creates CloudFront **standard logging v2** delivery resources and builds the
bucket-policy document that allows `delivery.logs.amazonaws.com` to write
objects into the log bucket under the prefix(es) configured for each
distribution.

> This module **does not modify** existing CloudFront distributions. The
> module creates CloudWatch Logs delivery v2 source/destination/binding
> resources in `us-east-1`, which is the required control-plane region for
> CloudFront logging.

## Usage

```hcl
module "cloudfront_logging" {
  source                  = "../../modules/cloudfront-logging"
  bucket_arn              = module.log_bucket.bucket_arn
  account_id              = data.aws_caller_identity.current.account_id
  distribution_ids        = ["E12ABCDEFGHIJK", "E34LMNOPQRSTUV"]
  prefix_per_distribution = {
    "E12ABCDEFGHIJK" = "cloudfront/marketing-site/"
    "E34LMNOPQRSTUV" = "cloudfront/blog/"
  }
}
```

The module creates `aws_cloudwatch_log_delivery_source`,
`aws_cloudwatch_log_delivery_destination`,
`aws_cloudwatch_log_delivery_destination_policy`, and
`aws_cloudwatch_log_delivery` resources. The bucket policy is scoped to the
CloudWatch Logs delivery source ARNs and the current AWS account.

Do not use the legacy `aws_cloudfront_distribution.logging_config` path with
the shared `log-bucket` module: legacy CloudFront logging requires ACL-enabled
S3 buckets, while `log-bucket` intentionally uses bucket-owner-enforced object
ownership.

## Validation

The module fails fast if any prefix in `prefix_per_distribution` lacks a
trailing slash (CloudFront delivers under the literal prefix, so a missing `/`
silently puts objects at the bucket root).
