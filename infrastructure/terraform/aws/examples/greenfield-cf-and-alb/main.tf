terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.83" }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

module "log_bucket" {
  source             = "../../modules/log-bucket"
  bucket_name        = var.log_bucket_name
  log_retention_days = 90
  tags = {
    project = "llm-bot-pipeline"
    client  = var.client_name
  }
}

module "reader_role" {
  source             = "../../modules/reader-role"
  role_name          = coalesce(var.reader_role_name, "aws-logs-reader-${var.client_name}")
  bucket_arn         = module.log_bucket.bucket_arn
  gcp_sts_subject_id = var.gcp_sts_subject_id
  client_name        = var.client_name
}

module "cloudfront_logging" {
  count                   = length(var.cloudfront_distribution_ids) > 0 ? 1 : 0
  source                  = "../../modules/cloudfront-logging"
  bucket_arn              = module.log_bucket.bucket_arn
  account_id              = data.aws_caller_identity.current.account_id
  distribution_ids        = var.cloudfront_distribution_ids
  prefix_per_distribution = var.cloudfront_prefix_per_distribution
}

module "alb_logging" {
  count          = length(var.alb_arns) > 0 ? 1 : 0
  source         = "../../modules/alb-logging"
  bucket_arn     = module.log_bucket.bucket_arn
  alb_arns       = var.alb_arns
  prefix_per_alb = var.alb_prefix_per_alb
}

data "aws_iam_policy_document" "bucket" {
  source_policy_documents = concat(
    [for m in module.cloudfront_logging : m.policy_json],
    [for m in module.alb_logging : m.policy_json],
  )
}

resource "aws_s3_bucket_policy" "log_delivery" {
  bucket = module.log_bucket.bucket_name
  policy = data.aws_iam_policy_document.bucket.json
}

output "log_bucket_name" {
  value = module.log_bucket.bucket_name
}

output "log_bucket_arn" {
  value = module.log_bucket.bucket_arn
}

output "reader_role_arn" {
  value = module.reader_role.reader_role_arn
}
