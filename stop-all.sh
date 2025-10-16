#!/bin/bash

echo "=== Stopping Complete Telecom Analytics Platform ==="
echo "This will stop all services: core data pipeline and Airflow platform"
echo ""

COMPOSE_FILE="docker-compose.telecom.yml"

# Show what's currently running
echo "1. Currently running services:"
docker compose -f "$COMPOSE_FILE" ps --services --filter "status=running"

echo ""
read -p "Are you sure you want to stop ALL services? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Operation cancelled."
    exit 0
fi

# Stop all services
echo ""
echo "2. Stopping all services..."
docker compose -f "$COMPOSE_FILE" down

# Check if any containers are still running
echo ""
echo "3. Checking if all services are stopped..."
RUNNING_CONTAINERS=$(docker compose -f "$COMPOSE_FILE" ps --services --filter "status=running" | wc -l)

if [ "$RUNNING_CONTAINERS" -eq 0 ]; then
    echo "✅ All services stopped successfully!"
else
    echo "⚠️  Some services might still be running:"
    docker compose -f "$COMPOSE_FILE" ps --services --filter "status=running"
    echo ""
    echo "You can force stop with: docker compose -f $COMPOSE_FILE down --remove-orphans"
fi

echo ""
echo "=== Platform Status: STOPPED ==="
echo ""
echo "To start again: ./start-all.sh"
echo "Or start individually:"
echo "  Core pipeline only: ./start-system.sh"
echo "  Airflow only:       ./start-airflow.sh"
