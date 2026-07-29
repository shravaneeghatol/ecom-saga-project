#!/bin/bash
# ==============================================================================
# Dynamic IP Configuration Script for 2-Instance Architecture
# ==============================================================================
# Usage:
#   ./scripts/configure-monitoring-ips.sh monitoring  (Runs on Instance #2)
#   ./scripts/configure-monitoring-ips.sh app         (Runs on Instance #1)
# ==============================================================================
set -euo pipefail

ROLE="${1:-monitoring}"
REGION="${AWS_REGION:-ap-south-1}"

if [ "$ROLE" == "monitoring" ]; then
  echo "Configuring Prometheus scraping targets on Instance #2..."
  
  # Reset prometheus.yml to clean template
  git checkout -- config/prometheus.yml 2>/dev/null || true

  # Fetch Instance #1 IP
  APP_IP=$(aws ec2 describe-instances \
    --region "$REGION" \
    --filters "Name=tag:Name,Values=ecom-saga-app" "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].PublicIpAddress" \
    --output text 2>/dev/null || echo "")

  if [ -n "$APP_IP" ] && [ "$APP_IP" != "None" ]; then
    echo "Found Main App Instance #1 IP: $APP_IP"
    sed -i "s/order-service:8081/$APP_IP:8081/g" config/prometheus.yml
    sed -i "s/inventory-service:8082/$APP_IP:8082/g" config/prometheus.yml
    sed -i "s/payment-service:8083/$APP_IP:8083/g" config/prometheus.yml
    sed -i "s/notification-service:8084/$APP_IP:8084/g" config/prometheus.yml
    sed -i "s/node-exporter:9100/$APP_IP:9100/g" config/prometheus.yml
    sed -i "s/cadvisor:8080/$APP_IP:8080/g" config/prometheus.yml
    echo "Successfully configured config/prometheus.yml with target IP $APP_IP"
  else
    echo "⚠️ Warning: Could not resolve App Instance #1 IP. Using default container names."
  fi

elif [ "$ROLE" == "app" ]; then
  echo "Configuring Promtail log shipping target on Instance #1..."

  # Fetch Instance #2 IP
  MONITORING_IP=$(aws ec2 describe-instances \
    --region "$REGION" \
    --filters "Name=tag:Name,Values=ecom-saga-monitoring" "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].PublicIpAddress" \
    --output text 2>/dev/null || echo "")

  if [ -n "$MONITORING_IP" ] && [ "$MONITORING_IP" != "None" ]; then
    echo "Found Monitoring Instance #2 IP: $MONITORING_IP"
    if [ ! -f .env ]; then cp .env.example .env; fi
    sed -i "/LOKI_URL=/d" .env
    echo "LOKI_URL=http://$MONITORING_IP:3100/loki/api/v1/push" >> .env
    echo "Successfully updated .env with LOKI_URL=http://$MONITORING_IP:3100/loki/api/v1/push"
  else
    echo "⚠️ Warning: Could not resolve Monitoring Instance #2 IP. Using default LOKI_URL."
  fi
fi
