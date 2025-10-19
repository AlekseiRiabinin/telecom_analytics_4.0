#!/bin/bash

echo "=== Starting Debezium CDC Connector ==="

COMPOSE_FILE="docker-compose.telecom.yml"
CONNECTOR_FILE="./debezium/debezium-mssql-connector.json"
DEBEZIUM_PORT="8087"

# Start Debezium Connect
echo "1. Starting Debezium Connect service..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d debezium-connect

echo "2. Waiting for Debezium to initialize..."
echo "   This may take 2-3 minutes. Please wait..."
sleep 100

# Simple health check
echo "3. Checking if Debezium is responsive..."
if curl -s http://localhost:$DEBEZIUM_PORT/ > /dev/null; then
    echo "✅ Debezium is running"
else
    echo "❌ Debezium failed to start"
    echo "Checking logs..."
    docker compose -f "$COMPOSE_FILE" logs debezium-connect --tail=20
    exit 1
fi

# Delete any existing connector first
echo "4. Removing any existing connector..."
curl -s -X DELETE http://localhost:$DEBEZIUM_PORT/connectors/mssql-telecom-connector > /dev/null 2>&1 || true
sleep 5

# Create connector with timeout and monitoring
echo "5. Creating MSSQL CDC connector..."
echo "   This may take 1-2 minutes..."

# Start the connector creation in background and monitor
curl -X POST -H "Content-Type: application/json" \
  --data @$CONNECTOR_FILE \
  http://localhost:$DEBEZIUM_PORT/connectors &

CURL_PID=$!

# Wait for the curl command with timeout
timeout 60 wait $CURL_PID 2>/dev/null

if [ $? -eq 124 ]; then
    echo "⚠️  Connector creation is taking longer than expected..."
    echo "   Checking current status..."
else
    echo "✅ Connector creation completed"
fi

# Check connector status
echo "7. Checking connector status..."
sleep 10

connector_status=$(curl -s http://localhost:$DEBEZIUM_PORT/connectors/mssql-telecom-connector/status 2>/dev/null)

if [ -n "$connector_status" ]; then
    state=$(echo "$connector_status" | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
    
    case $state in
        "RUNNING")
            echo "🎉 SUCCESS: Connector is RUNNING!"
            echo "$connector_status" | jq .
            ;;
        "FAILED")
            echo "❌ Connector FAILED"
            echo "$connector_status" | jq .
            echo "Error details:"
            echo "$connector_status" | jq '.tasks[] | select(.state == "FAILED")'
            exit 1
            ;;
        *)
            echo "⚠️  Connector state: $state"
            echo "$connector_status" | jq .
            ;;
    esac
else
    echo "❌ Could not retrieve connector status"
    echo "Checking Debezium logs for errors..."
    docker compose -f "$COMPOSE_FILE" logs debezium-connect --tail=30 | grep -i error
fi

echo ""
echo "=== Debezium Setup Complete ==="
echo "Monitor at: http://localhost:$DEBEZIUM_PORT/connectors"
