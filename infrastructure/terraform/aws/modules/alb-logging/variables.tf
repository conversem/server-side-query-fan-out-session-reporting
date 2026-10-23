variable "bucket_arn" {
  description = "S3 bucket ARN (for resource targeting in the policy document)."
  type        = string
}

variable "alb_arns" {
  description = "List of ALB ARNs that will deliver logs. Used to scope the service-principal bucket policy."
  type        = list(string)
  validation {
    condition     = length(var.alb_arns) > 0
    error_message = "alb_arns must contain at least one ALB ARN."
  }
}

variable "prefix_per_alb" {
  description = "Map of ALB friendly-name → S3 prefix (without trailing slash). ALB appends '/AWSLogs/<account>/...' automatically."
  type        = map(string)
  validation {
    condition     = length(var.prefix_per_alb) > 0
    error_message = "prefix_per_alb must contain at least one ALB prefix."
  }
}
