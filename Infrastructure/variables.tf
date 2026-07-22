variable "aws_region" {
  description = "AWS region to deploy into. ap-south-1 (Mumbai) is closest to Pune/Nagpur."
  type        = string
  default     = "ap-south-1"
}

variable "instance_type" {
  description = <<-EOT
    Free-tier-eligible instance type. Check Billing -> Free Tier in the AWS console
    for your account's eligibility before changing this:
      - Accounts created BEFORE 2025-07-15: t2.micro or t3.micro (1 GiB RAM)
      - Accounts created ON/AFTER 2025-07-15: t3.micro, t3.small, t4g.micro, t4g.small
    t3.small / t4g.small (2 GiB RAM) is strongly recommended if it's available to you -
    Kafka + 4 Spring Boot JVMs need more than 1 GiB to run comfortably.
  EOT
  type        = string
  default     = "t3.small"
}

variable "root_volume_size_gb" {
  description = "Root EBS volume size in GB. Free Tier covers up to 30GB total."
  type        = number
  default     = 20
}

variable "my_ip_cidr" {
  description = "Your public IP in CIDR form (e.g. 203.0.113.4/32), used to restrict SSH and admin ports. Find yours at https://checkip.amazonaws.com"
  type        = string
}

variable "enable_ssh" {
  description = "If true, opens port 22 to my_ip_cidr and requires key_name. If false, use SSM Session Manager instead (no inbound ports at all)."
  type        = bool
  default     = true
}

variable "key_name" {
  description = "Name of an existing EC2 key pair, only required if enable_ssh = true."
  type        = string
  default     = ""
}

variable "expose_kafka_ui" {
  description = "If true, opens port 8090 (Kafka UI) to the entire internet (0.0.0.0/0). WARNING: Kafka UI has no built-in auth."
  type        = bool
  default     = true
}

variable "allocate_elastic_ip" {
  description = "If true, allocates and associates an Elastic IP (free while attached to a running instance)."
  type        = bool
  default     = false
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention in days. Keep short to avoid silent storage growth."
  type        = number
  default     = 14
}

variable "project_name" {
  description = "Used as a prefix/tag for all created resources."
  type        = string
  default     = "ecom-saga"
}
