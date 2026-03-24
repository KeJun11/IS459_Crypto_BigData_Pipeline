locals {
  glue_schema_definition = jsonencode(
    merge(
      jsondecode(file("${path.module}/../../schemas/kline_1m.schema.json")),
      {
        "$schema" = "http://json-schema.org/draft-07/schema#"
      }
    )
  )
}

resource "aws_glue_registry" "kline" {
  registry_name = var.glue_registry_name
  description   = "Schema registry for crypto kline messages."

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_glue_schema" "kline_1m" {
  schema_name       = var.glue_schema_name
  registry_arn      = aws_glue_registry.kline.arn
  data_format       = "JSON"
  compatibility     = "NONE"
  schema_definition = local.glue_schema_definition

  lifecycle {
    prevent_destroy = true
  }
}
