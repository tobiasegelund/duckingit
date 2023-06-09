resource "aws_lambda_layer_version" "this" {
  filename                 = var.src
  layer_name               = "duckdb"
  description              = "A installation of DuckDB binaries"
  compatible_architectures = ["arm64"]

  compatible_runtimes = ["python3.9"]
}
