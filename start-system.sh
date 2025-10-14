#!/bin/bash

echo "=== Starting Complete Kafka + ClickHouse Pipeline ==="

COMPOSE_FILE="docker-compose.telecom.yml"

# Function to check if service is running
is_service_running() {
    docker compose -f "$COMPOSE_FILE" ps "$1" | grep -q "Up"
}

# Function to start ClickHouse with proper waiting
start_clickhouse() {
    echo "Starting ClickHouse independently..."
    BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d --no-deps clickhouse
    
    echo "Waiting for ClickHouse to initialize..."
    sleep 20
    
    # Test ClickHouse
    echo "Testing ClickHouse..."
    if curl -s http://localhost:8123/ping | grep -q "Ok"; then
        echo "✅ ClickHouse is running and responding!"
        echo ""
        echo "Testing database access..."
        curl -s "http://admin:clickhouse_admin@localhost:8123?query=SHOW DATABASES"
        echo ""
        return 0
    else
        echo "❌ ClickHouse not responding. Checking logs..."
        docker compose -f "$COMPOSE_FILE" logs clickhouse --tail=10
        return 1
    fi
}

# Start Kafka brokers first
echo "1. Starting Kafka brokers..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d kafka-1 kafka-2

echo "Waiting for Kafka to initialize..."
sleep 25

# Create Kafka topic
echo "2. Creating Kafka topic..."
docker exec kafka-1 bash -c '
    if ! kafka-topics.sh --describe --topic "smart_meter_data" --bootstrap-server kafka-1:9092 >/dev/null 2>&1; then
        echo "Creating topic: smart_meter_data"
        kafka-topics.sh --create \
            --topic "smart_meter_data" \
            --partitions 4 \
            --replication-factor 2 \
            --bootstrap-server kafka-1:9092
    else
        echo "Topic smart_meter_data already exists."
    fi
'

# Start services that don't depend on health checks
echo "3. Starting MinIO and producer..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d minio kafka-producer minio-setup

sleep 10

# Start ClickHouse with proper method
echo "4. Starting ClickHouse..."
if start_clickhouse; then
    echo "✅ ClickHouse started successfully!"
else
    echo "⚠️  ClickHouse had issues starting, but continuing with pipeline..."
fi

echo "✅ Complete data pipeline deployment completed!"

# Final status check
echo ""
echo "=== Final Status ==="
docker compose -f "$COMPOSE_FILE" ps

echo ""
echo "=== Access Information ==="
echo "Kafka Brokers:    localhost:9092, localhost:9095"
echo "ClickHouse:       http://localhost:8123 (admin/clickhouse_admin)"
echo "MinIO Console:    http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "=== Next Steps ==="
echo "Check individual logs: docker compose -f $COMPOSE_FILE logs [service-name]"
echo "Stop all services:    ./stop-system.sh"
