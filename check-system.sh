#!/bin/bash

echo "=== System Status Verification ==="

COMPOSE_FILE="docker-compose.telecom.yml"

echo "1. Checking running services..."
docker compose -f "$COMPOSE_FILE" ps

echo ""
echo "2. Testing Kafka connectivity..."
docker exec kafka-1 kafka-topics.sh --list --bootstrap-server localhost:9092

echo ""
echo "3. Testing ClickHouse..."
if curl -s http://localhost:8123/ping | grep -q "Ok"; then
    echo "✅ ClickHouse is responding"
else
    echo "❌ ClickHouse not responding yet (may still be starting)"
fi

echo ""
echo "4. Testing MinIO..."
if curl -s http://localhost:9001/minio/health/live > /dev/null; then
    echo "✅ MinIO is responding"
else
    echo "❌ MinIO not responding yet (may still be starting)"
fi

echo ""
echo "5. Checking service logs for errors..."
echo "Kafka logs:"
docker compose -f "$COMPOSE_FILE" logs kafka-1 kafka-2 --tail=5

echo ""
echo "=== Next Steps ==="
echo "If services show as running above, your system is operational!"
echo "Access:"
echo "  Kafka: localhost:9092, localhost:9095"
echo "  ClickHouse: http://localhost:8123"
echo "  MinIO: http://localhost:9001"
