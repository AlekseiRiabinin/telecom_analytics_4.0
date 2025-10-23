#!/bin/bash

echo "=== Stopping Airflow Platform ==="

COMPOSE_FILE="docker-compose.telecom.yml"

# Stop Airflow services including Redis and Celery workers
docker compose -f "$COMPOSE_FILE" stop postgres redis airflow-init airflow-webserver airflow-scheduler airflow-worker-1 airflow-worker-2

echo "✅ Airflow platform stopped"
echo ""
echo "Note: Core services (Kafka, ClickHouse, MinIO) are still running"
echo "To stop all services: ./stop-system.sh"
