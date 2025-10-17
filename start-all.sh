#!/bin/bash

echo "=== Starting Complete Telecom Analytics Platform ==="
echo "This will start both the core data pipeline and Airflow platform"
echo ""

# Function to check if service is running
is_service_running() {
    docker compose -f "$COMPOSE_FILE" ps "$1" | grep -q "Up"
}

COMPOSE_FILE="docker-compose.telecom.yml"

# Check if services are already running
echo "1. Checking current status..."
RUNNING_SERVICES=$(docker compose -f "$COMPOSE_FILE" ps --services --filter "status=running" | wc -l)

if [ "$RUNNING_SERVICES" -gt 0 ]; then
    echo "⚠️  Some services are already running."
    echo "   Running services:"
    docker compose -f "$COMPOSE_FILE" ps --services --filter "status=running"
    echo ""
    read -p "Continue and start all services? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Exiting. Use individual scripts to control specific services."
        exit 0
    fi
fi

# Start core data pipeline
echo ""
echo "2. Starting core data pipeline (Kafka + ClickHouse + MinIO)..."
./start-system.sh

if [ $? -eq 0 ]; then
    echo "✅ Core data pipeline started successfully!"
else
    echo "⚠️  Core data pipeline had some issues, but continuing..."
fi

# Wait a bit for core services to stabilize
echo ""
echo "3. Waiting for core services to stabilize..."
sleep 15

# Start Spark cluster
echo ""
echo "4. Starting Spark cluster..."
./start-spark.sh

if [ $? -eq 0 ]; then
    echo "✅ Spark cluster started successfully!"
else
    echo "⚠️  Spark cluster had some issues, but continuing..."
fi

# Wait for Spark to initialize
echo ""
echo "5. Waiting for Spark to stabilize..."
sleep 10

# Start Debezium CDC
echo ""
echo "6. Starting Debezium CDC connector..."
./start-debezium.sh

if [ $? -eq 0 ]; then
    echo "✅ Debezium CDC started successfully!"
else
    echo "⚠️  Debezium CDC had some issues, but continuing..."
fi

# Start Airflow platform
echo ""
echo "7. Starting Airflow platform (PostgreSQL + Airflow)..."
./start-airflow.sh

if [ $? -eq 0 ]; then
    echo "✅ Airflow platform started successfully!"
else
    echo "⚠️  Airflow platform had some issues, but continuing..."
fi

# Final status check
echo ""
echo "8. Final platform status..."
echo "=== Complete Platform Status ==="
docker compose -f "$COMPOSE_FILE" ps


echo ""
echo "=== Access Information ==="
echo "📊 Data Pipeline:"
echo "   Kafka Brokers:    localhost:9092, localhost:9095"
echo "   ClickHouse:       http://localhost:8123 (admin/clickhouse_admin)"
echo "   MSSQL:            localhost:1433 (sa/Admin123!)"
echo "   MinIO Console:    http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "🔄 Change Data Capture:"
echo "   Debezium UI:      http://localhost:8087"
echo ""
echo "⚡ Processing:"
echo "   Spark Master:     http://localhost:8091"
echo ""
echo "⚙️  Orchestration:"
echo "   Airflow:          http://localhost:8083 (admin/admin)"
echo "   PostgreSQL:       localhost:5434 (postgres/postgres)"
echo ""
echo "=== Next Steps ==="
echo "🔍 Monitor pipeline:  ./verify-complete-pipeline.sh"
echo "📈 Run analytics:     ./run-analytics.sh"
echo "⚡ Submit Spark job:   ./submit-spark-job.sh"
echo "🔄 Test CDC:          ./test-cdc-pipeline.sh"
echo "🛑 Stop all:          ./stop-all.sh"
echo ""
echo "🎉 Telecom Analytics Platform is now running!"
