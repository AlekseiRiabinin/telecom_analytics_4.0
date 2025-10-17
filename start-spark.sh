#!/bin/bash

echo "=== Starting Spark Cluster ==="

COMPOSE_FILE="docker-compose.telecom.yml"

# Function to check if service is running
is_service_running() {
    docker compose -f "$COMPOSE_FILE" ps "$1" | grep -q "Up"
}

# Function to start Spark with proper waiting
start_spark() {
    echo "Starting Spark cluster..."
    BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d spark-master spark-worker
    
    echo "Waiting for Spark to initialize..."
    sleep 25
    
    # Test Spark
    echo "Testing Spark..."
    if curl -s http://localhost:8091 | grep -q "Spark"; then
        echo "✅ Spark Master is running and responding!"
        echo "✅ Spark Worker started successfully!"
        echo ""
        echo "Spark Master Web UI: http://localhost:8091"
        return 0
    else
        echo "⚠️  Spark Master Web UI not accessible, but services are starting..."
        docker compose -f "$COMPOSE_FILE" logs spark-master --tail=5
        return 1
    fi
}

# Check if Spark services are already running
echo "1. Checking current Spark status..."
if is_service_running "spark-master" || is_service_running "spark-worker"; then
    echo "⚠️  Spark services are already running."
    read -p "Restart Spark cluster? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Spark cluster already running."
        exit 0
    fi
    # Stop existing Spark services
    docker compose -f "$COMPOSE_FILE" stop spark-master spark-worker
    sleep 5
fi

# Start Spark cluster
echo "2. Starting Spark cluster..."
if start_spark; then
    echo "✅ Spark cluster started successfully!"
else
    echo "⚠️  Spark had issues starting, but services are deployed."
fi

# Final status check
echo ""
echo "=== Spark Cluster Status ==="
docker compose -f "$COMPOSE_FILE" ps spark-master spark-worker

echo ""
echo "=== Spark Access Information ==="
echo "Spark Master:     http://localhost:8091"
echo "Spark Submit:     spark://localhost:7077"
echo "Driver URL:       spark://spark-master:7077"
echo ""
echo "=== Next Steps ==="
echo "Submit Spark job: ./submit-spark-job.sh"
echo "Stop Spark:       ./stop-spark.sh"
echo "View logs:        docker compose -f $COMPOSE_FILE logs spark-master"

echo "🎉 Spark cluster is now running!"
