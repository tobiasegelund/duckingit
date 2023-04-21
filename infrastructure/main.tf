terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = var.region
}

module "lambda_layer" {
  source = "./modules/lambda_layer"

  src = "../image/release/duckdb-layer.zip"
}

module "sqs" {
  source = "./modules/sqs"

  delay_seconds             = 0
  max_message_size          = 2056
  message_retention_seconds = 900
  receive_wait_time_seconds = 0
}

module "lambda_function" {
  source = "./modules/lambda_function"

  src              = "./src"
  lambda_layer_arn = module.lambda_layer.lambda_layer_arn

  runtime     = "python3.9"
  timeout     = var.timeout
  memory_size = var.memory_size

  sqs_arn_failure = module.sqs.sqs_arn_failure
  sqs_arn_success = module.sqs.sqs_arn_success
}

variable "timeout" {
  type    = number
  default = 30
}

variable "memory_size" {
  type    = number
  default = 128
}
