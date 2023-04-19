resource "aws_sqs_queue" "failure" {
  name                      = "DuckFailure"
  delay_seconds             = var.delay_seconds
  max_message_size          = var.max_message_size          # 2 kB
  message_retention_seconds = var.message_retention_seconds # 15 minutes
  receive_wait_time_seconds = var.receive_wait_time_seconds # a value between 0 and 20 seconds
}


resource "aws_sqs_queue" "success" {
  name                      = "DuckSuccess"
  delay_seconds             = var.delay_seconds
  max_message_size          = var.max_message_size          # 2 kB
  message_retention_seconds = var.message_retention_seconds # 15 minutes
  receive_wait_time_seconds = var.receive_wait_time_seconds # a value between 0 and 20 seconds
}

