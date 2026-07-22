#!/bin/bash

echo "Fetching the latest public IP of the EC2 instance..."

# Fetch the public IP of the running EC2 instance tagged with Name=ecom-saga-app
PUBLIC_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=ecom-saga-app" "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

if [ "$PUBLIC_IP" == "None" ] || [ -z "$PUBLIC_IP" ]; then
    echo "❌ Error: Could not find a running EC2 instance with tag Name=ecom-saga-app."
    exit 1
fi

echo ""
echo "==========================================================="
echo " 🚀 E-commerce Saga - Live Endpoints"
echo "==========================================================="
echo ""
echo " 🌐 Kafka UI Dashboard: http://${PUBLIC_IP}:8090"
echo ""
echo " 🩺 Health Checks:"
echo " Order Service Health: http://${PUBLIC_IP}:8081/actuator/health"
echo " Inventory Service Health: http://${PUBLIC_IP}:8082/actuator/health"
echo " Payment Service Health: http://${PUBLIC_IP}:8083/actuator/health"
echo " Notification Service Health: http://${PUBLIC_IP}:8084/actuator/health"
echo ""
echo " 🗄️ H2 Database Consoles:"
echo " Order Service DB: http://${PUBLIC_IP}:8081/h2-console"
echo " Inventory Service DB: http://${PUBLIC_IP}:8082/h2-console"
echo " Payment Service DB: http://${PUBLIC_IP}:8083/h2-console"
echo " Notification Service DB: http://${PUBLIC_IP}:8084/h2-console"
echo "==========================================================="
echo " JDBC URL Pattern for DB Login: jdbc:h2:mem:<dbname>"
echo " (e.g. jdbc:h2:mem:orderdb, User: sa, Password: <blank>)"
echo "==========================================================="
