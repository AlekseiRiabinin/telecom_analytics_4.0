#!/bin/bash

echo "=== Starting Debezium CDC Connector ==="

COMPOSE_FILE="docker-compose.telecom.yml"
CONNECTOR_FILE="debezium-mssql-connector.json"
DEBEZIUM_PORT="8087"  # NEW PORT

# Start Debezium Connect
echo "1. Starting Debezium Connect service..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d debezium-connect

echo "2. Waiting for Debezium to initialize..."
sleep 30

# Check if Debezium is ready
echo "3. Checking Debezium health..."
if curl -s http://localhost:$DEBEZIUM_PORT/connectors | grep -q "200"; then
    echo "✅ Debezium Connect is running"
else
    echo "❌ Debezium Connect not responding"
    docker compose -f "$COMPOSE_FILE" logs debezium-connect --tail=10
    exit 1
fi

# Register the connector
echo "4. Registering MSSQL CDC connector..."
response=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type:application/json" \
  http://localhost:$DEBEZIUM_PORT/connectors/ -d @$CONNECTOR_FILE)

if [ "$response" -eq 201 ] || [ "$response" -eq 409 ]; then
    echo "✅ Debezium connector registered successfully (HTTP $response)"
else
    echo "❌ Failed to register connector (HTTP $response)"
    exit 1
fi

# Verify connector status
echo "5. Verifying connector status..."
sleep 10
curl -s http://localhost:$DEBEZIUM_PORT/connectors/mssql-telecom-connector/status | jq .

echo ""
echo "🎉 Debezium CDC setup completed!"
echo "📊 CDC topics will be created in Kafka"
echo "🔍 Monitor at: http://localhost:$DEBEZIUM_PORT/connectors"
