terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.0" }
  }
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "AssumeRoleFromGoogleOIDC"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = ["accounts.google.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "accounts.google.com:sub"
      values   = [var.gcp_sts_subject_id]
    }
  }
}

data "aws_iam_policy_document" "read_logs" {
  statement {
    sid       = "ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.bucket_arn]
  }

  statement {
    sid       = "ReadObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectAttributes"]
    resources = ["${var.bucket_arn}/*"]
  }
}

resource "aws_iam_role" "reader" {
  name               = var.role_name
  assume_role_policy = data.aws_iam_policy_document.trust.json
  tags               = merge(var.tags, { client = var.client_name })
}

resource "aws_iam_role_policy" "read_logs" {
  name   = "${var.role_name}-read-logs"
  role   = aws_iam_role.reader.id
  policy = data.aws_iam_policy_document.read_logs.json
}
