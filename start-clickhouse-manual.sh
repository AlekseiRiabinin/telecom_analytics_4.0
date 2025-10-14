#!/bin/bash

echo "=== Starting ClickHouse (Bypassing Dependencies) ==="

COMPOSE_FILE="docker-compose.telecom.yml"

# Stop and remove the current ClickHouse container
echo "1. Stopping current ClickHouse container..."
docker compose -f "$COMPOSE_FILE" stop clickhouse
docker compose -f "$COMPOSE_FILE" rm -f clickhouse

# Start ClickHouse without dependency checks
echo "2. Starting ClickHouse independently..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d --no-deps clickhouse

echo "3. Waiting for ClickHouse to initialize..."
sleep 20

# Test ClickHouse
echo "4. Testing ClickHouse..."
if curl -s http://localhost:8123/ping | grep -q "Ok"; then
    echo "✅ ClickHouse is running and responding!"
    echo ""
    echo "Testing database access..."
    curl -s "http://admin:clickhouse_admin@localhost:8123?query=SHOW DATABASES"
    echo ""
    echo ""
    echo "✅ ClickHouse is fully operational!"
    echo "   HTTP Interface: http://localhost:8123"
    echo "   Native Interface: localhost:9000"
    echo "   Database: airflow"
    echo "   Username: admin"
    echo "   Password: clickhouse_admin"
else
    echo "❌ ClickHouse not responding. Checking logs..."
    docker compose -f "$COMPOSE_FILE" logs clickhouse --tail=20
fi
