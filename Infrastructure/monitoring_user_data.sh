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

# Clone repo
mkdir -p /home/ec2-user/app
if [ ! -d /home/ec2-user/app/.git ]; then
  git clone https://github.com/shravaneeghatol/ecom-saga-project.git /home/ec2-user/app
fi

cd /home/ec2-user/app
git fetch origin
git checkout feature-swagger || git checkout main
git pull origin feature-swagger || git pull origin main

chmod +x scripts/*.sh

# Configure Prometheus to scrape Instance #1 (retries until Instance #1 is up)
./scripts/configure-monitoring-ips.sh monitoring

# Start Monitoring Stack
/usr/local/bin/docker-compose -f docker-compose.monitoring.yml up -d

echo "Monitoring host bootstrap complete." > /var/log/bootstrap-done.log
