#!/bin/bash
# Bootstraps Docker + Docker Compose + Observability Stack on Instance #2 automatically.
set -euo pipefail

dnf update -y
dnf install -y docker git amazon-cloudwatch-agent amazon-ssm-agent

systemctl enable --now docker
systemctl enable --now amazon-ssm-agent
usermod -aG docker ec2-user

# Docker Compose v2 plugin
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
ln -sf /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

# Add swap file for memory safety
if [ ! -f /swapfile ]; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# Clone repo and start Monitoring Stack automatically on first boot
mkdir -p /home/ec2-user/app
if [ ! -d /home/ec2-user/app/.git ]; then
  git clone https://github.com/shravaneeghatol/ecom-saga-project.git /home/ec2-user/app
fi

cd /home/ec2-user/app
git fetch origin
git checkout feature-swagger || git checkout main
git pull origin feature-swagger || git pull origin main

# Dynamically discover Instance #1 IP and substitute in prometheus.yml
APP_IP=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=ecom-saga-app" "Name=instance-state-name,Values=running" --query "Reservations[0].Instances[0].PublicIpAddress" --output text 2>/dev/null || echo "")

if [ -n "$APP_IP" ] && [ "$APP_IP" != "None" ]; then
  sed -i "s/order-service:8081/$APP_IP:8081/g" config/prometheus.yml
  sed -i "s/inventory-service:8082/$APP_IP:8082/g" config/prometheus.yml
  sed -i "s/payment-service:8083/$APP_IP:8083/g" config/prometheus.yml
  sed -i "s/notification-service:8084/$APP_IP:8084/g" config/prometheus.yml
  sed -i "s/node-exporter:9100/$APP_IP:9100/g" config/prometheus.yml
  sed -i "s/cadvisor:8080/$APP_IP:8080/g" config/prometheus.yml
fi

/usr/local/bin/docker-compose -f docker-compose.monitoring.yml up -d

echo "Monitoring host bootstrap complete." > /var/log/bootstrap-done.log
