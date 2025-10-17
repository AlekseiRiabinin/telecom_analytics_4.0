#!/bin/bash

echo "=== Stopping Spark Cluster ==="

COMPOSE_FILE="docker-compose.telecom.yml"

echo "Stopping Spark services..."
docker compose -f "$COMPOSE_FILE" stop spark-master spark-worker

echo "✅ Spark cluster stopped!"
