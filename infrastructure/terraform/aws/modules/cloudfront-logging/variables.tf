variable "bucket_arn" {
  description = "S3 bucket ARN (for resource targeting in the policy document)."
  type        = string
}

variable "account_id" {
  description = "AWS account ID that owns the CloudFront distributions."
  type        = string
}

variable "distribution_ids" {
  description = "List of CloudFront distribution IDs that will deliver logs to the bucket. Used to build a tight bucket policy."
  type        = list(string)
}

variable "prefix_per_distribution" {
  description = "Map of distribution ID → S3 key prefix (with trailing slash). The client wires these into the CloudFront standard logging v2 delivery destination."
  type        = map(string)
  validation {
    condition = alltrue([
      for k, v in var.prefix_per_distribution : endswith(v, "/")
    ])
    error_message = "Each prefix must end with '/'."
  }
}

variable "delivery_name_prefix" {
  description = "Prefix used for CloudFront standard logging v2 delivery source/destination names."
  type        = string
  default     = "cf-access-logs"
}

variable "cloudwatch_logs_region" {
  description = "Region for CloudWatch Logs delivery v2 APIs. CloudFront requires us-east-1."
  type        = string
  default     = "us-east-1"
}

variable "output_format" {
  description = "CloudFront standard logging v2 output format for S3 delivery."
  type        = string
  default     = "w3c"
}
