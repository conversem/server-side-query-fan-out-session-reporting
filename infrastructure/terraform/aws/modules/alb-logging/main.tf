terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.0" }
  }
}

data "aws_caller_identity" "current" {}
data "aws_elb_service_account" "main" {} # region-aware: returns the right ELB log-delivery account for the current provider region.

data "aws_iam_policy_document" "alb_log_delivery" {
  # Policy 1: ELB service account (legacy, still used in some regions/ALB versions)
  statement {
    sid     = "AWSALBLogDeliveryViaELBAccount"
    effect  = "Allow"
    actions = ["s3:PutObject"]

    principals {
      type        = "AWS"
      identifiers = [data.aws_elb_service_account.main.arn]
    }

    resources = flatten([
      for name, prefix in var.prefix_per_alb : [
        "${var.bucket_arn}/${prefix}/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
      ]
    ])
  }

  # Policy 2: ALB log delivery service principal (newer regions / NLB / future)
  statement {
    sid     = "AWSALBLogDeliveryViaServicePrincipal"
    effect  = "Allow"
    actions = ["s3:PutObject"]

    principals {
      type        = "Service"
      identifiers = ["logdelivery.elasticloadbalancing.amazonaws.com"]
    }

    resources = flatten([
      for name, prefix in var.prefix_per_alb : [
        "${var.bucket_arn}/${prefix}/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
      ]
    ])

    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-acl"
      values   = ["bucket-owner-full-control"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = var.alb_arns
    }
  }

  statement {
    sid     = "AWSALBLogDeliveryGetAcl"
    effect  = "Allow"
    actions = ["s3:GetBucketAcl"]

    principals {
      type        = "Service"
      identifiers = ["logdelivery.elasticloadbalancing.amazonaws.com"]
    }

    resources = [var.bucket_arn]
  }
}
