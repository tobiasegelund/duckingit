variable "src" {
  type = string
}

variable "lambda_layer_arn" {
  type = string
}

variable "runtime" {
  type = string
}

variable "timeout" {
  type = number
}

variable "memory_size" {
  type = number
}

variable "sqs_arn_success" {
  type = string
}

variable "sqs_arn_failure" {
  type = string
}
