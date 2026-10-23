variable "aws_region" {
  description = "AWS region to deploy into. Must match the ALB and log bucket region."
  type        = string
  default     = "eu-west-1"
}

variable "client_name" {
  description = "Short client identifier used in resource names."
  type        = string
}

variable "log_bucket_name" {
  description = "Globally unique S3 bucket name for ALB log delivery."
  type        = string
}

variable "reader_role_name" {
  description = "IAM role name assumed by GCP Storage Transfer Service. If unset, defaults to aws-logs-reader-<client_name>."
  type        = string
  default     = null
}

variable "gcp_sts_subject_id" {
  description = "Numeric subjectId of the Google-managed Storage Transfer Service service agent."
  type        = string
}

variable "alb_name" {
  description = "Name for the example Application Load Balancer."
  type        = string
}

variable "alb_internal" {
  description = "Whether the example ALB is internal."
  type        = bool
  default     = false
}

variable "alb_subnet_ids" {
  description = "Subnet IDs for the example ALB."
  type        = list(string)
}

variable "alb_vpc_id" {
  description = "VPC ID for the example ALB security group."
  type        = string
}

variable "alb_allowed_ingress_cidr_blocks" {
  description = "CIDR blocks allowed to reach the example ALB on HTTP."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "alb_log_prefix" {
  description = "S3 prefix for ALB access logs. Must not include AWSLogs."
  type        = string
  default     = "alb/entrypoint"

  validation {
    condition     = !strcontains(var.alb_log_prefix, "AWSLogs")
    error_message = "alb_log_prefix must not include AWSLogs because AWS appends that segment."
  }
}
