variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-west-1"
}

variable "client_name" {
  description = "Short client identifier used in resource names."
  type        = string
}

variable "log_bucket_name" {
  description = "Globally unique S3 bucket name for log delivery."
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

variable "cloudfront_distribution_ids" {
  description = "CloudFront distributions whose logs feed the bucket."
  type        = list(string)
  default     = []
}

variable "cloudfront_prefix_per_distribution" {
  description = "Map of distribution ID → S3 prefix (trailing slash)."
  type        = map(string)
  default     = {}
}

variable "alb_arns" {
  description = "ALBs whose logs feed the bucket."
  type        = list(string)
  default     = []
}

variable "alb_prefix_per_alb" {
  description = "Map of ALB friendly-name → S3 prefix (no trailing slash)."
  type        = map(string)
  default     = {}
}
