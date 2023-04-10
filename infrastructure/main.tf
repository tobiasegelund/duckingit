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
  timeout     = 30
  memory_size = 128
}
