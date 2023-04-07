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

module "lambda_function" {
  source = "./modules/lambda_function"

  src = "./src/lambda_handler.py"
}
