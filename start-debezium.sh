#!/bin/bash

echo "=== Starting Debezium CDC Connector ==="

COMPOSE_FILE="docker-compose.telecom.yml"
CONNECTOR_FILE="./debezium/debezium-mssql-connector.json"
DEBEZIUM_PORT="8087"

# Verify Kafka cluster is healthy first
echo "0. Verifying Kafka cluster health..."
if ! docker exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server kafka-1:19092 >/dev/null 2>&1; then
    echo "❌ Kafka cluster is not healthy. Please run ./start-system.sh first"
    exit 1
fi

echo "✅ Kafka cluster is healthy"

# Create schema history topic
echo "0. Creating schema history topic..."
docker exec kafka-1 kafka-topics.sh --delete --topic schema-history.telecom_db --bootstrap-server kafka-1:19092 2>/dev/null || true
sleep 2

docker exec kafka-1 kafka-topics.sh --create \
  --topic schema-history.telecom_db \
  --partitions 1 \
  --replication-factor 2 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --bootstrap-server kafka-1:19092

echo "✅ Schema history topic created"

# Start Debezium Connect
echo "1. Starting Debezium Connect service..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d debezium-connect

echo "2. Waiting for Debezium to initialize..."
echo "   This may take 2-3 minutes. Please wait..."

# Health check with retries
max_attempts=12
attempt=1
while [ $attempt -le $max_attempts ]; do
    if curl -s http://localhost:$DEBEZIUM_PORT/ > /dev/null; then
        echo "✅ Debezium is running and responsive (attempt $attempt/$max_attempts)"
        break
    else
        echo "⏳ Debezium not ready yet (attempt $attempt/$max_attempts)..."
        sleep 10
        ((attempt++))
    fi
done

if [ $attempt -gt $max_attempts ]; then
    echo "❌ Debezium failed to start within expected time"
    echo "Checking logs..."
    docker compose -f "$COMPOSE_FILE" logs debezium-connect --tail=30
    exit 1
fi

# Delete any existing connector first
echo "3. Removing any existing connector..."
curl -s -X DELETE http://localhost:$DEBEZIUM_PORT/connectors/mssql-telecom-connector > /dev/null 2>&1 || true
sleep 5

# Create connector with error handling
echo "4. Creating MSSQL CDC connector..."
echo "   This may take 1-2 minutes..."

response=$(curl -s -w "%{http_code}" -X POST -H "Content-Type: application/json" \
  --data @$CONNECTOR_FILE \
  http://localhost:$DEBEZIUM_PORT/connectors)

http_code=${response: -3}
response_body=${response%???}

if [ "$http_code" -eq 201 ] || [ "$http_code" -eq 409 ]; then
    echo "✅ Connector creation request accepted (HTTP $http_code)"
else
    echo "❌ Connector creation failed (HTTP $http_code)"
    echo "Response: $response_body"
    echo "Checking Debezium logs..."
    docker compose -f "$COMPOSE_FILE" logs debezium-connect --tail=20
    exit 1
fi

# Check connector status with retries
echo "5. Checking connector status..."
max_checks=6
check_attempt=1

while [ $check_attempt -le $max_checks ]; do
    connector_status=$(curl -s http://localhost:$DEBEZIUM_PORT/connectors/mssql-telecom-connector/status 2>/dev/null)
    
    if [ -n "$connector_status" ]; then
        state=$(echo "$connector_status" | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
        
        case $state in
            "RUNNING")
                echo "🎉 SUCCESS: Connector is RUNNING!"
                echo "$connector_status" | jq .
                break
                ;;
            "FAILED")
                echo "❌ Connector FAILED"
                echo "$connector_status" | jq .
                echo "Error details:"
                echo "$connector_status" | jq '.tasks[] | select(.state == "FAILED")'
                exit 1
                ;;
            *)
                echo "⚠️  Connector state: $state (check $check_attempt/$max_checks)"
                if [ $check_attempt -eq $max_checks ]; then
                    echo "Final status: $state"
                    echo "$connector_status" | jq .
                fi
                ;;
        esac
    else
        echo "⏳ Could not retrieve connector status (check $check_attempt/$max_checks)"
    fi
    
    if [ "$state" != "RUNNING" ]; then
        sleep 10
        ((check_attempt++))
    else
        break
    fi
done

echo ""
echo "=== Debezium Setup Complete ==="
echo "Monitor at: http://localhost:$DEBEZIUM_PORT/connectors"
