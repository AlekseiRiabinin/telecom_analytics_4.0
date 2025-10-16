#!/bin/bash

echo "=== Telecom Analytics Platform Status ==="

COMPOSE_FILE="docker-compose.telecom.yml"

echo "1. All Services Status:"
docker compose -f "$COMPOSE_FILE" ps

echo ""
echo "2. Service Health Check:"

# Core services
CORE_SERVICES=("kafka-1" "kafka-2" "clickhouse-server" "minio")
echo "   📊 Core Data Pipeline:"
for service in "${CORE_SERVICES[@]}"; do
    if docker compose -f "$COMPOSE_FILE" ps "$service" | grep -q "Up"; then
        echo "      ✅ $service: RUNNING"
    else
        echo "      ❌ $service: STOPPED"
    fi
done

# Airflow services
AIRFLOW_SERVICES=("postgres" "airflow-webserver" "airflow-scheduler")
echo ""
echo "   ⚙️  Airflow Platform:"
for service in "${AIRFLOW_SERVICES[@]}"; do
    if docker compose -f "$COMPOSE_FILE" ps "$service" | grep -q "Up"; then
        echo "      ✅ $service: RUNNING"
    else
        echo "      ❌ $service: STOPPED"
    fi
done

# Check web interfaces
echo ""
echo "3. Web Interfaces:"
if curl -s http://localhost:8083/health 2>/dev/null | grep -q "healthy"; then
    echo "   ✅ Airflow:     http://localhost:8083"
else
    echo "   ❌ Airflow:     NOT ACCESSIBLE"
fi

if curl -s http://localhost:8123/ping 2>/dev/null | grep -q "Ok"; then
    echo "   ✅ ClickHouse:  http://localhost:8123"
else
    echo "   ❌ ClickHouse:  NOT ACCESSIBLE"
fi

if curl -s http://localhost:9001/minio/health/live 2>/dev/null; then
    echo "   ✅ MinIO:       http://localhost:9001"
else
    echo "   ❌ MinIO:       NOT ACCESSIBLE"
fi

echo ""
echo "=== Platform Summary ==="
TOTAL_SERVICES=$(docker compose -f "$COMPOSE_FILE" ps --services | wc -l)
RUNNING_SERVICES=$(docker compose -f "$COMPOSE_FILE" ps --services --filter "status=running" | wc -l)

echo "   Services: $RUNNING_SERVICES/$TOTAL_SERVICES running"
if [ "$RUNNING_SERVICES" -eq "$TOTAL_SERVICES" ]; then
    echo "   🟢 STATUS: FULLY OPERATIONAL"
elif [ "$RUNNING_SERVICES" -gt 0 ]; then
    echo "   🟡 STATUS: PARTIALLY RUNNING"
else
    echo "   🔴 STATUS: STOPPED"
fi
