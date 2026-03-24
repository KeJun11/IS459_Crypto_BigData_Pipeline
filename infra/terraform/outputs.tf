output "s3_bucket_name" {
  description = "Data lake bucket name."
  value       = aws_s3_bucket.datalake.bucket
}

output "s3_prefixes" {
  description = "Standard S3 prefixes for Bronze, Silver, checkpoints, and Firehose errors."
  value       = local.s3_prefixes
}

output "kinesis_stream_name" {
  description = "Name of the Kinesis Data Stream."
  value       = aws_kinesis_stream.ohlcv.name
}

output "kinesis_stream_arn" {
  description = "ARN of the Kinesis Data Stream."
  value       = aws_kinesis_stream.ohlcv.arn
}

output "firehose_delivery_stream_name" {
  description = "Name of the Firehose delivery stream."
  value       = aws_kinesis_firehose_delivery_stream.bronze_archive.name
}

output "firehose_delivery_stream_arn" {
  description = "ARN of the Firehose delivery stream."
  value       = aws_kinesis_firehose_delivery_stream.bronze_archive.arn
}

output "glue_registry_arn" {
  description = "ARN of the Glue Schema Registry."
  value       = aws_glue_registry.kline.arn
}

output "glue_schema_arn" {
  description = "ARN of the Glue schema."
  value       = aws_glue_schema.kline_1m.arn
}

output "ec2_instance_id" {
  description = "EC2 instance ID."
  value       = aws_instance.app.id
}

output "ec2_public_ip" {
  description = "EC2 public IP address."
  value       = aws_instance.app.public_ip
}

output "ec2_public_dns" {
  description = "EC2 public DNS name."
  value       = aws_instance.app.public_dns
}

output "ec2_private_ip" {
  description = "EC2 private IP address."
  value       = aws_instance.app.private_ip
}

output "ec2_role_arn" {
  description = "IAM role ARN attached to the EC2 instance profile."
  value       = aws_iam_role.ec2.arn
}

output "firehose_role_arn" {
  description = "IAM role ARN used by Firehose."
  value       = aws_iam_role.firehose.arn
}

output "emr_serverless_execution_role_arn" {
  description = "IAM execution role ARN for EMR Serverless jobs."
  value       = aws_iam_role.emr_serverless_execution.arn
}

output "emr_serverless_application_id" {
  description = "EMR Serverless application ID."
  value       = aws_emrserverless_application.spark.id
}

output "ec2_security_group_id" {
  description = "Security group protecting the EC2 instance."
  value       = aws_security_group.ec2.id
}

output "ec2_docker_data_volume_id" {
  description = "Persistent EBS volume ID used for Docker and ClickHouse data."
  value       = aws_ebs_volume.docker_data.id
}

output "emr_serverless_security_group_id" {
  description = "Security group attached to EMR Serverless."
  value       = aws_security_group.emr_serverless.id
}
