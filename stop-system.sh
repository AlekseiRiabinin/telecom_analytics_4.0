#!/bin/bash
echo "Stopping all Telecom services..."
docker compose -f docker-compose.telecom.yml down
echo "✅ All services stopped!"
