#!/bin/bash
# ==============================================================================
# Dynamic IP Configuration Script for 2-Instance Architecture
# ==============================================================================
# Usage (run from the repo root on the EC2 instance):
#   ./scripts/configure-monitoring-ips.sh monitoring  (Runs on Instance #2)
#   ./scripts/configure-monitoring-ips.sh app         (Runs on Instance #1)
# ==============================================================================
set -euo pipefail

ROLE="${1:-monitoring}"
REGION="${AWS_REGION:-ap-south-1}"

# Retry helper: retry a command up to N times with a delay
retry() {
  local n=0
  local max=$1
  local delay=$2
  shift 2
  until "$@"; do
    n=$((n + 1))
    if [ "$n" -ge "$max" ]; then
      echo "❌ Command failed after $max attempts: $*"
      return 1
    fi
    echo "⏳ Attempt $n/$max failed. Retrying in ${delay}s..."
    sleep "$delay"
  done
}

if [ "$ROLE" == "monitoring" ]; then
  echo "=== Configuring Prometheus scraping targets on Instance #2 ==="

  # Reset prometheus.yml to clean git template FIRST so sed always finds container names
  git checkout -- config/prometheus.yml 2>/dev/null || true

  # Fetch Instance #1 IP — retry for up to 5 minutes in case it is still booting
  echo "Waiting for ecom-saga-app instance to be running and have a public IP..."
  APP_IP=""
  for i in $(seq 1 30); do
    APP_IP=$(aws ec2 describe-instances \
      --region "$REGION" \
      --filters "Name=tag:Name,Values=ecom-saga-app" "Name=instance-state-name,Values=running" \
      --query "Reservations[0].Instances[0].PublicIpAddress" \
      --output text 2>/dev/null || echo "")
    if [ -n "$APP_IP" ] && [ "$APP_IP" != "None" ]; then
      echo "✅ Found App Instance #1 IP: $APP_IP"
      break
    fi
    echo "⏳ Attempt $i/30: Instance #1 IP not yet available. Retrying in 10s..."
    sleep 10
  done

  if [ -z "$APP_IP" ] || [ "$APP_IP" == "None" ]; then
    echo "❌ Could not resolve App Instance #1 IP after 5 minutes. Exiting."
    exit 1
  fi

  # Substitute container names with real IP in prometheus.yml
  sed -i "s|order-service:8081|${APP_IP}:8081|g" config/prometheus.yml
  sed -i "s|inventory-service:8082|${APP_IP}:8082|g" config/prometheus.yml
  sed -i "s|payment-service:8083|${APP_IP}:8083|g" config/prometheus.yml
  sed -i "s|notification-service:8084|${APP_IP}:8084|g" config/prometheus.yml
  sed -i "s|node-exporter:9100|${APP_IP}:9100|g" config/prometheus.yml
  sed -i "s|cadvisor:8080|${APP_IP}:8080|g" config/prometheus.yml

  echo "✅ config/prometheus.yml updated with IP ${APP_IP}:"
  grep -E "targets:|  - '" config/prometheus.yml || grep "targets" config/prometheus.yml

elif [ "$ROLE" == "app" ]; then
  echo "=== Configuring Promtail log shipping target on Instance #1 ==="

  # Ensure .env exists
  if [ ! -f .env ]; then cp .env.example .env; fi

  # Fetch Instance #2 IP — retry for up to 5 minutes in case it is still booting
  echo "Waiting for ecom-saga-monitoring instance to be running and have a public IP..."
  MONITORING_IP=""
  for i in $(seq 1 30); do
    MONITORING_IP=$(aws ec2 describe-instances \
      --region "$REGION" \
      --filters "Name=tag:Name,Values=ecom-saga-monitoring" "Name=instance-state-name,Values=running" \
      --query "Reservations[0].Instances[0].PublicIpAddress" \
      --output text 2>/dev/null || echo "")
    if [ -n "$MONITORING_IP" ] && [ "$MONITORING_IP" != "None" ]; then
      echo "✅ Found Monitoring Instance #2 IP: $MONITORING_IP"
      break
    fi
    echo "⏳ Attempt $i/30: Instance #2 IP not yet available. Retrying in 10s..."
    sleep 10
  done

  if [ -z "$MONITORING_IP" ] || [ "$MONITORING_IP" == "None" ]; then
    echo "⚠️  Warning: Could not resolve Monitoring Instance #2 IP. Promtail will use default loki:3100 (only works on single-host)."
    exit 0
  fi

  # Write LOKI_URL into .env — remove any existing LOKI_URL line first
  sed -i '/^LOKI_URL=/d' .env
  echo "LOKI_URL=http://${MONITORING_IP}:3100/loki/api/v1/push" >> .env
  echo "✅ Updated .env: LOKI_URL=http://${MONITORING_IP}:3100/loki/api/v1/push"

else
  echo "❌ Unknown role: $ROLE. Use 'monitoring' or 'app'."
  exit 1
fi
