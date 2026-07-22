#!/bin/bash
# Bootstraps Docker + Docker Compose + CloudWatch Agent on first boot.
# Amazon Linux 2023 assumed (matches the AMI data source in main.tf).
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

# Minimal CloudWatch Agent config: ship system metrics + docker logs.
# Adjust the log_group_name below to match aws_cloudwatch_log_group.app in main.tf.
cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json <<'CWCONFIG'
{
  "metrics": {
    "namespace": "${project_name}",
    "metrics_collected": {
      "cpu": { "measurement": ["cpu_usage_active"], "totalcpu": true },
      "mem": { "measurement": ["mem_used_percent"] },
      "disk": { "measurement": ["used_percent"], "resources": ["/"] }
    }
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/lib/docker/containers/*/*.log",
            "log_group_name": "/${project_name}/app",
            "log_stream_name": "{instance_id}"
          }
        ]
      }
    }
  }
}
CWCONFIG

# Use the agent control utility to load the config and start the agent.
# This is required to ensure the custom JSON config above is actually loaded;
# `systemctl enable --now` alone only starts the agent without applying config.
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

# Add a swap file - cheap insurance against OOM kills on small instance types.
if [ ! -f /swapfile ]; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# NOTE: application deployment itself is intentionally left to the CI/CD
# pipeline (see ci-cd/deploy.yml) or a manual `git clone` + `docker compose up -d`,
# so that redeploys don't require re-running Terraform / rebooting the instance.
echo "Bootstrap complete. Clone the app repo and run docker compose to deploy." > /var/log/bootstrap-done.log
