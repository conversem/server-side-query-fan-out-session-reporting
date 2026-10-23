output "policy_json" {
  description = "Policy document JSON. Merge in the example composition."
  value       = data.aws_iam_policy_document.alb_log_delivery.json
}

output "prefix_per_alb" {
  description = "Echo of the input mapping. The client copies this into each ALB's access_logs block."
  value       = var.prefix_per_alb
}
