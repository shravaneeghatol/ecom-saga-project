terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# Default VPC / subnet - deliberately NOT creating a new VPC or NAT Gateway.
# A NAT Gateway alone costs ~$32+/month, which defeats the "near $0" goal,
# and this single-instance design doesn't need one (the instance has its own
# public IP for outbound package/image pulls).
# ---------------------------------------------------------------------------
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ---------------------------------------------------------------------------
# Security Group
# ---------------------------------------------------------------------------
resource "aws_security_group" "app" {
  name        = "${var.project_name}-sg"
  description = "Security group for the ${var.project_name} single-instance deployment"
  vpc_id      = data.aws_vpc.default.id

  # App ports: order/inventory/payment/notification services.
  # Tighten cidr_blocks to var.my_ip_cidr instead of 0.0.0.0/0 while testing
  # if you don't want the APIs open to the whole internet yet.
  ingress {
    description = "Spring Boot services"
    from_port   = 8081
    to_port     = 8084
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  dynamic "ingress" {
    for_each = var.enable_ssh ? [1] : []
    content {
      description = "SSH (restricted to your IP)"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }

  dynamic "ingress" {
    for_each = var.expose_kafka_ui ? [1] : []
    content {
      description = "Kafka UI (open to everyone)"
      from_port   = 8090
      to_port     = 8090
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-sg" }
}

# ---------------------------------------------------------------------------
# IAM role - least privilege: CloudWatch Logs/Metrics, SSM (for Session
# Manager instead of SSH), and optional ECR read if you switch off Docker Hub.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "instance" {
  name = "${var.project_name}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cloudwatch_agent" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_role_policy_attachment" "ssm_managed" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "ecr_read" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_instance_profile" "instance" {
  name = "${var.project_name}-instance-profile"
  role = aws_iam_role.instance.name
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group - short retention to avoid silent storage growth
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "app" {
  name              = "/${var.project_name}/app"
  retention_in_days = var.log_retention_days
}

# ---------------------------------------------------------------------------
# EC2 instance
# ---------------------------------------------------------------------------
resource "aws_instance" "app" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.app.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name
  key_name               = var.enable_ssh ? var.key_name : null

  root_block_device {
    volume_size = var.root_volume_size_gb
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_tokens = "required" # IMDSv2 only
  }

  user_data = templatefile("${path.module}/user_data.sh", {
    project_name = var.project_name
  })

  tags = { Name = "${var.project_name}-app" }
}

# ---------------------------------------------------------------------------
# Elastic IP - free only while attached to a RUNNING instance. If you stop
# the instance to save money, either release the EIP or accept the small
# hourly charge for an unattached/idle one.
# ---------------------------------------------------------------------------
resource "aws_eip" "app" {
  count    = var.allocate_elastic_ip ? 1 : 0
  instance = aws_instance.app.id
  domain   = "vpc"
  tags     = { Name = "${var.project_name}-eip" }
}

# EC2 status-check auto-recovery alarm - free, self-healing on instance
# hardware/status failures.
resource "aws_cloudwatch_metric_alarm" "status_check_failed" {
  alarm_name          = "${var.project_name}-status-check-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "StatusCheckFailed_System"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Maximum"
  threshold           = 0
  dimensions          = { InstanceId = aws_instance.app.id }
  alarm_actions       = ["arn:aws:automate:${var.aws_region}:ec2:recover"]
}

resource "aws_cloudwatch_metric_alarm" "cpu_high" {
  alarm_name          = "${var.project_name}-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 5
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Average"
  threshold           = 80
  dimensions          = { InstanceId = aws_instance.app.id }
  alarm_description   = "CPU above 80% for 5 consecutive minutes"
}
