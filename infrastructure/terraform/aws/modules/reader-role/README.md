# reader-role

Creates an IAM role that the GCP pipeline's Storage Transfer Service can
assume via OIDC federated identity — no long-lived AWS access keys cross the
cloud boundary.

## How the trust works

1. The GCP Storage Transfer Service job is given the role ARN as
   `awsS3DataSource.roleArn`.
2. The Google-managed Storage Transfer Service service agent obtains a Google
   OIDC ID token whose `sub` claim is that service agent's numeric `subjectId`.
4. AWS STS verifies the token signature against `accounts.google.com` (built-in
   OIDC provider, no extra IAM OIDC provider resource needed).
5. The role's trust policy requires `accounts.google.com:sub` ==
   `var.gcp_sts_subject_id` — only that STS service agent can assume it.

> `gcp_sts_subject_id` is the numeric `subjectId` returned for the Storage
> Transfer Service service agent, not the service-agent email and not the
> Cloud Run pipeline service account unique ID.

## Usage

```hcl
module "reader_role" {
  source           = "../../modules/reader-role"
  role_name        = "aws-logs-reader-acme"
  bucket_arn       = module.log_bucket.bucket_arn
  gcp_sts_subject_id = "1234567890123456789"
  client_name      = "acme"
}
```

## Policy scope

- `s3:ListBucket`, `s3:GetBucketLocation` on the bucket
- `s3:GetObject`, `s3:GetObjectAttributes` on every object

No write permissions. No delete. No bucket policy mutation.

## Rotation / revocation

- If the Storage Transfer Service service agent subject changes, apply the
  module with the new `gcp_sts_subject_id`.
- Revoke immediately by setting the role's trust policy to an empty list of
  principals via `terraform apply -replace=aws_iam_role.reader` after editing
  `main.tf`.
