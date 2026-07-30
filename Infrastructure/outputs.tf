output "instance_id" {
  description = "EC2 instance ID (needed for the GitHub Actions deploy.yml SSM step)"
  value       = aws_instance.app.id
}

output "public_ip" {
  description = "Public IP address of the app instance"
  value       = aws_instance.app.public_ip
}

output "ssh_command" {
  description = "SSH command (only useful if enable_ssh = true)"
  value       = var.enable_ssh ? "ssh -i ${var.key_name}.pem ec2-user@${aws_instance.app.public_ip}" : "SSH disabled - use: aws ssm start-session --target ${aws_instance.app.id}"
}

output "service_urls" {
  description = "REST API base URLs for each service once the app is deployed"
  value = {
    order_service        = "http://${aws_instance.app.public_ip}:8081/api/orders"
    inventory_service    = "http://${aws_instance.app.public_ip}:8082/api/inventory"
    payment_service      = "http://${aws_instance.app.public_ip}:8083"
    notification_service = "http://${aws_instance.app.public_ip}:8084/api/notifications"
  }
}

output "swagger_ui_urls" {
  description = "Swagger UI documentation URLs for each service once deployed"
  value = {
    order_service        = "http://${aws_instance.app.public_ip}:8081/swagger-ui.html"
    inventory_service    = "http://${aws_instance.app.public_ip}:8082/swagger-ui.html"
    payment_service      = "http://${aws_instance.app.public_ip}:8083/swagger-ui.html"
    notification_service = "http://${aws_instance.app.public_ip}:8084/swagger-ui.html"
  }
}

output "monitoring_instance_id" {
  description = "Monitoring EC2 instance ID"
  value       = aws_instance.monitoring.id
}

output "monitoring_public_ip" {
  description = "Public IP address of the monitoring instance"
  value       = aws_instance.monitoring.public_ip
}

output "observability_urls" {
  description = "Observability stack endpoints"
  value = {
    grafana    = "http://${aws_instance.monitoring.public_ip}:3000"
    prometheus = "http://${aws_instance.monitoring.public_ip}:9090"
  }
}

output "cloudwatch_log_group" {
  value = aws_cloudwatch_log_group.app.name
}
