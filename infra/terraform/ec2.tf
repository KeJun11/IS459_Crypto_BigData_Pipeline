data "aws_ssm_parameter" "amazon_linux_2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_key_pair" "ec2" {
  key_name   = "${local.name_prefix}-ec2-key"
  public_key = var.ssh_public_key
}

resource "aws_ebs_volume" "docker_data" {
  availability_zone = aws_subnet.public[0].availability_zone
  size              = var.ec2_docker_data_volume_size_gb
  type              = "gp3"
  encrypted         = true

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-docker-data"
    }
  )
}

resource "aws_instance" "app" {
  ami                         = data.aws_ssm_parameter.amazon_linux_2023.value
  instance_type               = var.ec2_instance_type
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  key_name                    = aws_key_pair.ec2.key_name
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/templates/ec2_bootstrap.sh.tftpl", {
    docker_data_mount_point = var.ec2_docker_data_mount_point
    repo_path               = var.ec2_bootstrap_repo_path
    repo_ref                = var.ec2_bootstrap_repo_ref
    repo_url                = var.ec2_bootstrap_repo_url
    swap_size_gb            = var.ec2_swap_size_gb
  })

  root_block_device {
    volume_size           = var.root_volume_size_gb
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  tags = merge(
    local.common_tags,
    {
      Name = "${local.name_prefix}-app"
    }
  )

  metadata_options {
    http_tokens = "required"
  }
}

resource "aws_volume_attachment" "docker_data" {
  device_name                    = var.ec2_docker_data_device_name
  volume_id                      = aws_ebs_volume.docker_data.id
  instance_id                    = aws_instance.app.id
  stop_instance_before_detaching = true
}
