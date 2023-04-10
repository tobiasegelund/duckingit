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

module "lambda_function" {
  source = "./modules/lambda_function"

  src              = "./src"
  lambda_layer_arn = module.lambda_layer.lambda_layer_arn

  runtime     = "python3.9"
  timeout     = var.timeout
  memory_size = var.memory_size
}

variable "timeout" {
  type    = number
  default = 30
}

variable "memory_size" {
  type    = number
  default = 128
}
