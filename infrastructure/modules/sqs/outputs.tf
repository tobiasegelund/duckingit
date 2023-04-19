output "sqs_arn_failure" {
  value = aws_sqs_queue.failure.arn
}

output "sqs_arn_success" {
  value = aws_sqs_queue.success.arn
}
