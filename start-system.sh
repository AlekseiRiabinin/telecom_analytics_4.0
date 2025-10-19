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
        echo "ClickHouse is running and responding!"
        echo ""
        echo "Testing database access..."
        curl -s "http://admin:clickhouse_admin@localhost:8123?query=SHOW DATABASES"
        echo ""
        return 0
    else
        echo "ClickHouse not responding. Checking logs..."
        docker compose -f "$COMPOSE_FILE" logs clickhouse --tail=10
        return 1
    fi
}

# Function to start MSSQL with proper waiting
start_mssql() {
    echo "Starting MSSQL independently..."
    BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d --no-deps mssql
    
    echo "Waiting for MSSQL to initialize..."
    sleep 30
    
    # Test MSSQL
    echo "Testing MSSQL..."
    if docker exec mssql-server /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "Admin123!" -Q "SELECT 1" -C -b >/dev/null 2>&1; then
        echo "MSSQL is running and responding!"
        echo ""
        echo "Testing database access..."
        docker exec mssql-server /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "Admin123!" -Q "SELECT name FROM sys.databases" -C
        echo ""
        return 0
    else
        echo "MSSQL not responding. Checking logs..."
        docker compose -f "$COMPOSE_FILE" logs mssql --tail=10
        return 1
    fi
}

# Function to create Kafka Connect internal topics
create_connect_topics() {
    echo "Creating Kafka Connect internal topics..."
    docker exec kafka-1 bash -c '
        kafka-topics.sh --delete --topic debezium_connect_configs --bootstrap-server kafka-1:9092 2>/dev/null || true
        kafka-topics.sh --delete --topic debezium_connect_offsets --bootstrap-server kafka-1:9092 2>/dev/null || true
        kafka-topics.sh --delete --topic debezium_connect_statuses --bootstrap-server kafka-1:9092 2>/dev/null || true
        
        sleep 2
        
        echo "Creating Kafka Connect internal topics with replication factor 2..."
        kafka-topics.sh --create \
            --topic debezium_connect_configs \
            --partitions 1 \
            --replication-factor 2 \
            --config cleanup.policy=compact \
            --bootstrap-server kafka-1:9092
            
        kafka-topics.sh --create \
            --topic debezium_connect_offsets \
            --partitions 25 \
            --replication-factor 2 \
            --config cleanup.policy=compact \
            --bootstrap-server kafka-1:9092
            
        kafka-topics.sh --create \
            --topic debezium_connect_statuses \
            --partitions 5 \
            --replication-factor 2 \
            --config cleanup.policy=compact \
            --bootstrap-server kafka-1:9092
            
        echo "Kafka Connect internal topics created successfully"
    '
}

# Start MSSQL first (it takes the longest to initialize)
echo "1. Starting MSSQL database..."
if start_mssql; then
    echo "MSSQL started successfully!"
else
    echo "MSSQL had issues starting, but continuing with pipeline..."
fi

# Start Kafka brokers first
echo "2. Starting Kafka brokers..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d kafka-1 kafka-2

echo "Waiting for Kafka to initialize..."
sleep 25

# Create Kafka Connect internal topics FIRST
echo "3. Creating Kafka Connect internal topics..."
create_connect_topics

# Create application topics
echo "4. Creating application Kafka topics..."
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

    if ! kafka-topics.sh --describe --topic "telecom-cdc.dbo.smart_meter_data" --bootstrap-server kafka-1:9092 >/dev/null 2>&1; then
        echo "Creating CDC topic: telecom-cdc.dbo.smart_meter_data"
        kafka-topics.sh --create \
            --topic "telecom-cdc.dbo.smart_meter_data" \
            --partitions 4 \
            --replication-factor 2 \
            --bootstrap-server kafka-1:9092
    else
        echo "Topic telecom-cdc.dbo.smart_meter_data already exists."
    fi    

    if ! kafka-topics.sh --describe --topic "dbhistory.telecom_db" --bootstrap-server kafka-1:9092 >/dev/null 2>&1; then
        echo "Creating CDC history topic: dbhistory.telecom_db"
        kafka-topics.sh --create \
            --topic "dbhistory.telecom_db" \
            --partitions 1 \
            --replication-factor 2 \
            --bootstrap-server kafka-1:9092
    else
        echo "Topic dbhistory.telecom_db already exists."
    fi
'

# Start services that don't depend on health checks
echo "5. Starting MinIO and producer..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d minio kafka-producer minio-setup

sleep 10

# Start ClickHouse with proper method
echo "6. Starting ClickHouse..."
if start_clickhouse; then
    echo "ClickHouse started successfully!"
else
    echo "ClickHouse had issues starting, but continuing with pipeline..."
fi

echo "Complete data pipeline deployment completed!"

# Final status check
echo ""
echo "=== Final Status ==="
docker compose -f "$COMPOSE_FILE" ps

echo ""
echo "=== Access Information ==="
echo "Kafka Brokers:    localhost:9092, localhost:9095"
echo "ClickHouse:       http://localhost:8123 (admin/clickhouse_admin)"
echo "MSSQL:            localhost:1433 (sa/Admin123!)"
echo "MinIO Console:    http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "=== Next Steps ==="
echo "Check individual logs: docker compose -f $COMPOSE_FILE logs [service-name]"
echo "Stop all services:    ./stop-system.sh"
