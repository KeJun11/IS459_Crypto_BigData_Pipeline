locals {
  name_prefix = "${var.project_name}-${var.environment}"

  public_ports = [22, 3000, 8080, 8081, 8123, 9000]

  s3_prefixes = {
    bronze_rest     = "bronze/rest/"
    bronze_stream   = "bronze/stream/"
    silver          = "silver/"
    checkpoints     = "checkpoints/"
    firehose_errors = "firehose-errors/"
  }

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Repository  = "IS459_Crypto_BigData_Pipeline"
  }
}
