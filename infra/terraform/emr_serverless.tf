resource "aws_emrserverless_application" "spark" {
  name          = "${local.name_prefix}-spark"
  release_label = var.emr_release_label
  type          = "SPARK"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  maximum_capacity {
    cpu    = "8 vCPU"
    memory = "32 GB"
    disk   = "200 GB"
  }

  network_configuration {
    subnet_ids         = aws_subnet.public[*].id
    security_group_ids = [aws_security_group.emr_serverless.id]
  }
}
