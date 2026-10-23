terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.0" }
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

resource "aws_security_group" "alb" {
  name_prefix = "${var.alb_name}-"
  description = "Example ALB security group for ${var.client_name}"
  vpc_id      = var.alb_vpc_id

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.alb_allowed_ingress_cidr_blocks
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    project = "llm-bot-pipeline"
    client  = var.client_name
  }
}

resource "aws_lb" "entrypoint" {
  name               = var.alb_name
  internal           = var.alb_internal
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.alb_subnet_ids

  access_logs {
    bucket  = module.log_bucket.bucket_name
    prefix  = var.alb_log_prefix
    enabled = true
  }

  tags = {
    project = "llm-bot-pipeline"
    client  = var.client_name
  }
}

module "alb_logging" {
  source         = "../../modules/alb-logging"
  bucket_arn     = module.log_bucket.bucket_arn
  alb_arns       = [aws_lb.entrypoint.arn]
  prefix_per_alb = { (var.alb_name) = var.alb_log_prefix }
}

data "aws_iam_policy_document" "bucket" {
  source_policy_documents = [module.alb_logging.policy_json]
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

output "alb_arn" {
  value = aws_lb.entrypoint.arn
}

output "alb_log_s3_prefix" {
  value = "${var.alb_log_prefix}/AWSLogs/${data.aws_caller_identity.current.account_id}/elasticloadbalancing/${var.aws_region}/"
}
