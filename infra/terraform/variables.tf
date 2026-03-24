variable "aws_region" {
  description = "AWS region for all infrastructure."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short project identifier used in resource names and tags."
  type        = string
}

variable "environment" {
  description = "Deployment environment name, such as dev or prod."
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 data lake bucket."
  type        = string
  default     = "is459-crypto-datalake"
}

variable "kinesis_stream_name" {
  description = "Name of the Kinesis Data Stream for live OHLCV records."
  type        = string
  default     = "crypto-ohlcv-1m"
}

variable "firehose_delivery_stream_name" {
  description = "Name of the Firehose delivery stream archiving Kinesis data into S3."
  type        = string
  default     = "crypto-ohlcv-1m-firehose"
}

variable "glue_registry_name" {
  description = "Name of the Glue Schema Registry."
  type        = string
  default     = "crypto-schema-registry"
}

variable "glue_schema_name" {
  description = "Name of the Glue schema resource for kline records."
  type        = string
  default     = "kline-1m"
}

variable "ec2_instance_type" {
  description = "Instance type for the Docker-hosted application node."
  type        = string
  default     = "t3.xlarge"
}

variable "admin_cidrs" {
  description = "CIDR blocks allowed to reach the EC2 host and exposed service ports."
  type        = list(string)

  validation {
    condition     = length(var.admin_cidrs) > 0
    error_message = "admin_cidrs must include at least one CIDR block."
  }
}

variable "ssh_public_key" {
  description = "SSH public key material used to create the EC2 key pair."
  type        = string

  validation {
    condition     = can(regex("^ssh-(rsa|ed25519|ecdsa)\\s+", var.ssh_public_key))
    error_message = "ssh_public_key must be a valid SSH public key string."
  }
}

variable "vpc_cidr" {
  description = "CIDR block for the Terraform-managed VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "Two public subnet CIDR blocks used by EC2 and EMR Serverless."
  type        = list(string)
  default     = ["10.42.0.0/24", "10.42.1.0/24"]

  validation {
    condition     = length(var.public_subnet_cidrs) == 2
    error_message = "public_subnet_cidrs must contain exactly two CIDR blocks."
  }
}

variable "root_volume_size_gb" {
  description = "Root EBS volume size for the EC2 instance."
  type        = number
  default     = 40
}

variable "ec2_swap_size_gb" {
  description = "Swap file size created during EC2 bootstrap."
  type        = number
  default     = 4
}

variable "ec2_docker_data_volume_size_gb" {
  description = "Persistent EBS volume size used for Docker data, including ClickHouse volumes."
  type        = number
  default     = 80
}

variable "ec2_docker_data_device_name" {
  description = "Device name used when attaching the persistent Docker data EBS volume."
  type        = string
  default     = "/dev/sdf"
}

variable "ec2_docker_data_mount_point" {
  description = "Mount point on EC2 used as Docker's persistent data-root."
  type        = string
  default     = "/mnt/docker-data"
}

variable "ec2_bootstrap_repo_url" {
  description = "Optional Git URL cloned onto the EC2 instance during bootstrap."
  type        = string
  default     = ""
}

variable "ec2_bootstrap_repo_ref" {
  description = "Git branch or tag checked out on EC2 when ec2_bootstrap_repo_url is set."
  type        = string
  default     = "main"
}

variable "ec2_bootstrap_repo_path" {
  description = "Target directory for the optional repo clone on the EC2 instance."
  type        = string
  default     = "/opt/is459-crypto-bigdata-pipeline"
}

variable "emr_release_label" {
  description = "EMR Serverless release label for the Spark application."
  type        = string
  default     = "emr-6.15.0"
}
