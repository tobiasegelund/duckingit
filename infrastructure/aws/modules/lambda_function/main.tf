resource "aws_iam_role" "this" {
  name = "DuckDBExecutor"

  assume_role_policy = <<EOF
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Action": "sts:AssumeRole",
          "Principal": {
            "Service": "lambda.amazonaws.com"
          },
          "Effect": "Allow",
          "Sid": ""
        }
      ]
    }
EOF
}

resource "aws_iam_role_policy_attachment" "this" {
  for_each = toset(
    [
      "arn:aws:iam::aws:policy/AmazonS3FullAccess",
      "arn:aws:iam::aws:policy/CloudWatchFullAccess",
      "arn:aws:iam::aws:policy/AmazonSQSFullAccess"
    ]
  )
  role       = aws_iam_role.this.name
  policy_arn = each.value
}

data "archive_file" "this" {
  type        = "zip"
  source_file = "${var.src}/lambda_handler.py"
  output_path = "${var.src}/lambda_handler.zip"
}


resource "aws_lambda_function" "this" {
  function_name = "DuckExecutor"
  filename      = "${var.src}/lambda_handler.zip"
  architectures = ["arm64"]
  handler       = "lambda_handler.lambda_handler"
  role          = aws_iam_role.this.arn
  runtime       = var.runtime
  timeout       = var.timeout
  memory_size   = var.memory_size

  layers = [var.lambda_layer_arn]

  ephemeral_storage {
    size = 512
  }
}


resource "aws_lambda_function_event_invoke_config" "this" {
  function_name          = aws_lambda_function.this.arn
  maximum_retry_attempts = 0
  qualifier              = "$LATEST"

  destination_config {
    on_failure {
      destination = var.sqs_arn_failure
    }

    on_success {
      destination = var.sqs_arn_success
    }
  }

}
