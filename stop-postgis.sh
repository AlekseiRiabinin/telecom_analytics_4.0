#!/bin/bash

echo "=== Stopping PostGIS CRE Stack ==="

COMPOSE_FILE="docker-compose.telecom.yml"

services=(
  postgis-cre
  pgadmin-cre
  graphql-cre
)

for service in "${services[@]}"; do
    if docker compose -f "$COMPOSE_FILE" config --services | grep -q "^$service$"; then
        echo "Stopping $service..."
        docker compose -f "$COMPOSE_FILE" stop "$service"
    else
        echo "Skipping $service (not defined)"
    fi
done

echo ""
echo "CRE services stopped successfully"
