variable "bucket_name" {
  description = "S3 bucket name (globally unique). e.g. aws-logs-<client>-<random>."
  type        = string
}

variable "tags" {
  description = "Tags applied to the bucket."
  type        = map(string)
  default     = {}
}

variable "log_retention_days" {
  description = "Lifecycle rule: delete log objects older than N days."
  type        = number
  default     = 90
}

variable "transition_to_ia_days" {
  description = "Lifecycle rule: transition logs to STANDARD_IA after N days. 0 disables."
  type        = number
  default     = 30
}
