#!/bin/bash

echo "=== Stopping Debezium CDC Connector ==="

COMPOSE_FILE="docker-compose.telecom.yml"

# Delete the connector first
echo "1. Deleting Debezium connector..."
curl -s -X DELETE http://localhost:8083/connectors/mssql-telecom-connector

echo "2. Stopping Debezium Connect service..."
docker compose -f "$COMPOSE_FILE" stop debezium-connect

echo "✅ Debezium CDC connector stopped!"
