#!/bin/bash
# Bootstraps Docker + Docker Compose + Observability Stack on Instance #2.
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

echo "Monitoring host bootstrap complete." > /var/log/bootstrap-done.log
