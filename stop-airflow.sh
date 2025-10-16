#!/bin/bash

echo "=== Stopping Airflow Platform ==="

COMPOSE_FILE="docker-compose.telecom.yml"

# Stop only Airflow and PostgreSQL services
docker compose -f "$COMPOSE_FILE" stop postgres airflow-init airflow-webserver airflow-scheduler

echo "✅ Airflow platform stopped"
echo ""
echo "Note: Core services (Kafka, ClickHouse, MinIO) are still running"
echo "To stop all services: ./stop-system.sh"
