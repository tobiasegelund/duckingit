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
  for_each   = toset(["arn:aws:iam::aws:policy/AmazonS3FullAccess", "arn:aws:iam::aws:policy/CloudWatchFullAccess"])
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
  architectures = ["x86_64"]
  handler       = "index.lambda_handler"
  role          = aws_iam_role.this.arn
  runtime       = var.runtime
  timeout       = var.timeout
  memory_size   = var.memory_size

  layers = []

  ephemeral_storage {
    size = 512
  }
}
