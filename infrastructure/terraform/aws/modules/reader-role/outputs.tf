output "reader_role_arn" {
  description = "ARN of the reader role; pass to GCP STS as awsS3DataSource.roleArn."
  value       = aws_iam_role.reader.arn
}
