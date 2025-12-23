#!/bin/bash
set -e

echo "=== Starting PostGIS CRE Stack ==="

COMPOSE_FILE="docker-compose.telecom.yml"

# Check if service exists in compose
service_exists() {
    docker compose -f "$COMPOSE_FILE" config --services | grep -q "^$1$"
}

# Check if service is running
is_service_running() {
    docker compose -f "$COMPOSE_FILE" ps "$1" | grep -q "Up"
}

# Wait for a container to become healthy
wait_for_health() {
    local service=$1
    local attempts=10
    for i in $(seq 1 $attempts); do
        if [ "$(docker inspect -f '{{.State.Health.Status}}' "$service" 2>/dev/null)" = "healthy" ]; then
            echo "$service is ready"
            return 0
        fi
        echo "Waiting for $service... ($i/$attempts)"
        sleep 5
    done
    echo "$service failed to become healthy"
    docker compose -f "$COMPOSE_FILE" logs "$service" --tail=20
    exit 1
}

# Start PostGIS
start_postgis() {
    echo "Starting PostGIS (CRE)..."
    docker compose -f "$COMPOSE_FILE" up -d --no-deps postgis-cre
    wait_for_health postgis-cre
}

# Start pgAdmin
start_pgadmin() {
    echo "Starting pgAdmin (CRE)..."
    docker compose -f "$COMPOSE_FILE" up -d --no-deps pgadmin-cre
    sleep 10
    echo "pgAdmin started"
}

# Start Redis
start_redis() {
    if ! service_exists redis-cre; then
        echo "redis-cre service not defined — skipping"
        return
    fi

    echo "Starting Redis (CRE)..."
    docker compose -f "$COMPOSE_FILE" up -d --no-deps redis-cre

    # Simple health check via PING
    local attempts=10
    for i in $(seq 1 $attempts); do
        if docker exec redis-cre redis-cli ping >/dev/null 2>&1; then
            echo "Redis is ready"
            return 0
        fi
        echo "Waiting for Redis... ($i/$attempts)"
        sleep 3
    done
    echo "Redis failed to start"
    docker compose -f "$COMPOSE_FILE" logs redis-cre --tail=20
    exit 1
}

# Start GraphQL (optional)
start_graphql() {
    if ! service_exists graphql-cre; then
        echo "graphql-cre service not defined — skipping"
        return
    fi

    # Check if image exists locally
    if ! docker image inspect graphql-cre >/dev/null 2>&1; then
        echo "graphql-cre image not built yet — skipping startup"
        echo "(This is expected until GraphQL service is implemented)"
        return
    fi

    echo "Starting GraphQL CRE service..."
    docker compose -f "$COMPOSE_FILE" up -d --no-deps graphql-cre
    echo "GraphQL CRE service started"
}

# Execution order
start_postgis
start_pgadmin
start_redis
start_graphql

echo ""
echo "=== CRE Stack Status ==="
docker compose -f "$COMPOSE_FILE" ps postgis-cre pgadmin-cre redis-cre graphql-cre || true

echo ""
echo "=== Access Information ==="
echo "PostGIS:   localhost:5435 (cre_user / cre_password)"
echo "pgAdmin:   http://localhost:5051"
echo "Redis:     localhost:6380"
echo "GraphQL:   http://localhost:8088/graphql (when implemented)"
echo ""
echo "CRE PostGIS stack started successfully"
