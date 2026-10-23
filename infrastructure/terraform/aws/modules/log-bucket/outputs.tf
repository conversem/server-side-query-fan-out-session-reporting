output "bucket_name" {
  description = "Name of the log bucket."
  value       = aws_s3_bucket.log_bucket.id
}

output "bucket_arn" {
  description = "ARN of the log bucket."
  value       = aws_s3_bucket.log_bucket.arn
}

output "bucket_regional_domain_name" {
  description = "Regional domain name for integrations that need it."
  value       = aws_s3_bucket.log_bucket.bucket_regional_domain_name
}
