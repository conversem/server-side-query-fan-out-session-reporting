terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.83" }
  }
}

locals {
  distribution_arns = {
    for dist_id in var.distribution_ids :
    dist_id => "arn:aws:cloudfront::${var.account_id}:distribution/${dist_id}"
  }

  destination_arns = {
    for dist_id in var.distribution_ids :
    dist_id => "${var.bucket_arn}/${trimsuffix(var.prefix_per_distribution[dist_id], "/")}"
  }
}

resource "aws_cloudwatch_log_delivery_source" "cloudfront" {
  for_each = local.distribution_arns

  region       = var.cloudwatch_logs_region
  name         = "${var.delivery_name_prefix}-${each.key}"
  log_type     = "ACCESS_LOGS"
  resource_arn = each.value
}

resource "aws_cloudwatch_log_delivery_destination" "cloudfront" {
  for_each = local.destination_arns

  region        = var.cloudwatch_logs_region
  name          = "${var.delivery_name_prefix}-${each.key}-s3"
  output_format = var.output_format

  delivery_destination_configuration {
    destination_resource_arn = each.value
  }
}

data "aws_iam_policy_document" "delivery_destination" {
  for_each = aws_cloudwatch_log_delivery_destination.cloudfront

  statement {
    sid     = "AllowCreateDelivery"
    effect  = "Allow"
    actions = ["logs:CreateDelivery"]

    principals {
      type        = "AWS"
      identifiers = [var.account_id]
    }

    resources = [each.value.arn]
  }
}

resource "aws_cloudwatch_log_delivery_destination_policy" "cloudfront" {
  for_each = aws_cloudwatch_log_delivery_destination.cloudfront

  region                      = var.cloudwatch_logs_region
  delivery_destination_name   = each.value.name
  delivery_destination_policy = data.aws_iam_policy_document.delivery_destination[each.key].json
}

resource "aws_cloudwatch_log_delivery" "cloudfront" {
  for_each = local.distribution_arns

  region                   = var.cloudwatch_logs_region
  delivery_source_name     = aws_cloudwatch_log_delivery_source.cloudfront[each.key].name
  delivery_destination_arn = aws_cloudwatch_log_delivery_destination.cloudfront[each.key].arn

  depends_on = [aws_cloudwatch_log_delivery_destination_policy.cloudfront]
}

data "aws_iam_policy_document" "cloudfront_log_delivery" {
  statement {
    sid     = "AWSCloudFrontLogDelivery"
    effect  = "Allow"
    actions = ["s3:PutObject"]

    principals {
      type        = "Service"
      identifiers = ["delivery.logs.amazonaws.com"]
    }

    resources = flatten([
      for dist_id in var.distribution_ids : [
        "${var.bucket_arn}/${lookup(var.prefix_per_distribution, dist_id, "cloudfront/${dist_id}/")}*"
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
      values   = [var.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [for source in values(aws_cloudwatch_log_delivery_source.cloudfront) : source.arn]
    }
  }

  statement {
    sid     = "AWSCloudFrontLogDeliveryGetAcl"
    effect  = "Allow"
    actions = ["s3:GetBucketAcl"]

    principals {
      type        = "Service"
      identifiers = ["delivery.logs.amazonaws.com"]
    }

    resources = [var.bucket_arn]
  }
}
