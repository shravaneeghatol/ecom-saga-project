output "instance_id" {
  description = "EC2 instance ID (needed for the GitHub Actions deploy.yml SSM step)"
  value       = aws_instance.app.id
}

output "public_ip" {
  description = "Public IP address of the instance"
  value       = var.allocate_elastic_ip ? join("", aws_eip.app[*].public_ip) : aws_instance.app.public_ip
}

output "ssh_command" {
  description = "SSH command (only useful if enable_ssh = true)"
  value       = var.enable_ssh ? "ssh -i ${var.key_name}.pem ec2-user@${var.allocate_elastic_ip ? join("", aws_eip.app[*].public_ip) : aws_instance.app.public_ip}" : "SSH disabled - use: aws ssm start-session --target ${aws_instance.app.id}"
}

output "service_urls" {
  description = "REST API base URLs for each service once the app is deployed"
  value = {
    order_service        = "http://${var.allocate_elastic_ip ? join("", aws_eip.app[*].public_ip) : aws_instance.app.public_ip}:8081/api/orders"
    inventory_service    = "http://${var.allocate_elastic_ip ? join("", aws_eip.app[*].public_ip) : aws_instance.app.public_ip}:8082/api/inventory"
    payment_service      = "http://${var.allocate_elastic_ip ? join("", aws_eip.app[*].public_ip) : aws_instance.app.public_ip}:8083"
    notification_service = "http://${var.allocate_elastic_ip ? join("", aws_eip.app[*].public_ip) : aws_instance.app.public_ip}:8084/api/notifications"
  }
}

output "cloudwatch_log_group" {
  value = aws_cloudwatch_log_group.app.name
}
