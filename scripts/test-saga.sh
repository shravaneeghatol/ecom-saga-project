#!/usr/bin/env bash
# Exercises every saga path against the running docker-compose stack.
# Usage: ./scripts/test-saga.sh

set -e

# Try to fetch the EC2 instance IP if it's running
PUBLIC_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=ecom-saga-app" "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text 2>/dev/null || echo "")

if [ "$PUBLIC_IP" == "None" ] || [ -z "$PUBLIC_IP" ]; then
    HOST="localhost"
    echo "Testing against local environment (localhost)..."
else
    HOST="$PUBLIC_IP"
    echo "Testing against live EC2 environment ($HOST)..."
fi

ORDER_URL="http://${HOST}:8081/api/orders"

post_order () {
  echo ">> POST $1"
  curl -s -X POST "$ORDER_URL" -H "Content-Type: application/json" -d "$1" | python3 -m json.tool
  echo
}

echo "============================================================"
echo "1) HAPPY PATH - should end with status COMPLETED in order-db"
echo "   and a matching row in inventory / payment / notification DBs"
echo "============================================================"
post_order '{
  "customerId": "cust-1",
  "productId": "PROD-1",
  "quantity": 2,
  "amount": 49.99
}'

sleep 2

echo "============================================================"
echo "2) BUSINESS FAILURE - insufficient stock (immediate compensation,"
echo "   no retries). Order should end CANCELLED."
echo "============================================================"
post_order '{
  "customerId": "cust-2",
  "productId": "PROD-OUT-OF-STOCK",
  "quantity": 1,
  "amount": 19.99
}'

sleep 2

echo "============================================================"
echo "3) BUSINESS FAILURE - payment declined (inventory reserved, then"
echo "   released as compensation). Order should end CANCELLED."
echo "============================================================"
post_order '{
  "customerId": "cust-3",
  "productId": "PROD-1",
  "quantity": 1,
  "amount": 9.99,
  "simulatePaymentFailure": true
}'

sleep 2

echo "============================================================"
echo "4) TRANSIENT FAILURE in inventory-service - exercises the 3 retry"
echo "   topics then the DLT topic (order.events-retry-0/1/2, order.events-dlt)."
echo "   Watch it live in Kafka UI: http://${HOST}:8090"
echo "   Order should end CANCELLED after retries are exhausted."
echo "============================================================"
post_order '{
  "customerId": "cust-4",
  "productId": "PROD-1",
  "quantity": 1,
  "amount": 29.99,
  "simulateTransientErrorAt": "inventory"
}'

echo "Done. Check http://${HOST}:8090 (Kafka UI) and:"
echo "  GET http://${HOST}:8081/api/orders"
echo "  GET http://${HOST}:8082/api/inventory/reservations"
echo "  GET http://${HOST}:8083/api/payments"
echo "  GET http://${HOST}:8084/api/notifications"
