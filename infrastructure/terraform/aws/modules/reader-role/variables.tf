variable "role_name" {
  description = "IAM role name (e.g. aws-logs-reader-<client>)."
  type        = string
}

variable "bucket_arn" {
  description = "ARN of the log bucket the role can read."
  type        = string
}

variable "gcp_sts_subject_id" {
  description = "Subject ID of the Google-managed Storage Transfer Service service agent returned by storagetransfer.projects.getServiceAccount / googleServiceAccounts.get."
  type        = string
  validation {
    condition     = can(regex("^[0-9]+$", var.gcp_sts_subject_id))
    error_message = "gcp_sts_subject_id must be the numeric subjectId for the Storage Transfer Service service agent, not an email."
  }
}

variable "client_name" {
  description = "Tag value for cost-attribution."
  type        = string
}

variable "tags" {
  description = "Tags applied to the role."
  type        = map(string)
  default     = {}
}
