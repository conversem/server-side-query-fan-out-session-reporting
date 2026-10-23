output "policy_json" {
  description = "Policy document JSON. Merge with other policy documents in the example composition; the example creates ONE aws_s3_bucket_policy bound to the bucket."
  value       = data.aws_iam_policy_document.cloudfront_log_delivery.json
}

output "prefix_per_distribution" {
  description = "Echo of the input mapping. The client uses this when creating CloudFront standard logging v2 delivery destinations."
  value       = var.prefix_per_distribution
}

output "delivery_source_names" {
  description = "CloudFront standard logging v2 delivery source names by distribution ID."
  value = {
    for dist_id, source in aws_cloudwatch_log_delivery_source.cloudfront :
    dist_id => source.name
  }
}

output "delivery_destination_arns" {
  description = "CloudFront standard logging v2 delivery destination ARNs by distribution ID."
  value = {
    for dist_id, destination in aws_cloudwatch_log_delivery_destination.cloudfront :
    dist_id => destination.arn
  }
}
