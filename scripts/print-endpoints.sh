#!/bin/bash

echo "Fetching the latest public IP of the EC2 instance..."

# Load environment variables if .env exists
if [ -f .env ]; then
  # export to make sure variables are available, silencing errors for comments
  set -a
  source .env 2>/dev/null
  set +a
fi

DB_USER=${SPRING_DATASOURCE_USERNAME:-sa}
DB_PASS=${SPRING_DATASOURCE_PASSWORD:-<blank>}

# Fetch the public IP of the running EC2 instance tagged with Name=ecom-saga-app
PUBLIC_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=ecom-saga-app" "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

# Fetch the public IP of the monitoring EC2 instance tagged with Name=ecom-saga-monitoring
MONITORING_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=ecom-saga-monitoring" "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text 2>/dev/null || echo "")

if [ "$PUBLIC_IP" == "None" ] || [ -z "$PUBLIC_IP" ]; then
    echo "❌ Error: Could not find a running EC2 instance with tag Name=ecom-saga-app."
    exit 1
fi

echo ""
echo "==========================================================="
echo " 🚀 E-commerce Saga - Live Endpoints"
echo "==========================================================="
echo ""
echo " 🌐 Main App Instance (#1): http://${PUBLIC_IP}"
echo "    - Kafka UI Dashboard: http://${PUBLIC_IP}:8090"
echo "    - cAdvisor Metrics:   http://${PUBLIC_IP}:8080"
echo "    - Node Exporter:      http://${PUBLIC_IP}:9100"
echo ""
if [ -n "$MONITORING_IP" ] && [ "$MONITORING_IP" != "None" ]; then
  echo " 📊 Observability Instance (#2): http://${MONITORING_IP}"
  echo "    - Grafana Dashboards: http://${MONITORING_IP}:3000 (User: admin / Pass: admin)"
  echo "    - Prometheus Metrics: http://${MONITORING_IP}:9090"
  echo "    - Loki Log Engine:    http://${MONITORING_IP}:3100"
else
  echo " 📊 Observability Stack (Local): http://localhost:3000 (User: admin / Pass: admin)"
fi
echo ""
echo " 🩺 Health Checks:"
echo " Order Service Health: http://${PUBLIC_IP}:8081/actuator/health"
echo " Inventory Service Health: http://${PUBLIC_IP}:8082/actuator/health"
echo " Payment Service Health: http://${PUBLIC_IP}:8083/actuator/health"
echo " Notification Service Health: http://${PUBLIC_IP}:8084/actuator/health"
echo ""
echo " 📚 Swagger UI API Docs:"
echo " Order Service Swagger: http://${PUBLIC_IP}:8081/swagger-ui.html"
echo " Inventory Service Swagger: http://${PUBLIC_IP}:8082/swagger-ui.html"
echo " Payment Service Swagger: http://${PUBLIC_IP}:8083/swagger-ui.html"
echo " Notification Service Swagger: http://${PUBLIC_IP}:8084/swagger-ui.html"
echo ""
echo " 🗄️ H2 Database Consoles:"
echo " Order Service DB: http://${PUBLIC_IP}:8081/h2-console"
echo " Inventory Service DB: http://${PUBLIC_IP}:8082/h2-console"
echo " Payment Service DB: http://${PUBLIC_IP}:8083/h2-console"
echo " Notification Service DB: http://${PUBLIC_IP}:8084/h2-console"
echo "==========================================================="
echo " JDBC URL Pattern for DB Login: jdbc:h2:mem:<dbname>"
echo " (e.g. jdbc:h2:mem:orderdb, User: $DB_USER, Password: $DB_PASS)"
echo "==========================================================="
