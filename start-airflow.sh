#!/bin/bash

echo "=== Starting Airflow Platform (Fixed) ==="

COMPOSE_FILE="docker-compose.telecom.yml"

# Function to check if service is running
is_service_running() {
    docker compose -f "$COMPOSE_FILE" ps "$1" | grep -q "Up"
}

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    local max_attempts=30
    local attempt=1
    
    echo "Waiting for PostgreSQL to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if docker compose -f "$COMPOSE_FILE" exec postgres pg_isready -U postgres >/dev/null 2>&1; then
            echo "✅ PostgreSQL is ready!"
            return 0
        fi
        echo "Waiting for PostgreSQL... (attempt $attempt/$max_attempts)"
        sleep 5
        attempt=$((attempt + 1))
    done
    
    echo "❌ PostgreSQL not ready after $max_attempts attempts"
    return 1
}

# Function to wait for Airflow to be ready
wait_for_airflow() {
    local max_attempts=40
    local attempt=1
    
    echo "Waiting for Airflow webserver to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:8083/health 2>/dev/null | grep -q "healthy"; then
            echo "✅ Airflow webserver is ready!"
            return 0
        fi
        
        # Check if container is at least running
        if is_service_running "airflow-webserver"; then
            echo "Airflow container is up, waiting for web interface... (attempt $attempt/$max_attempts)"
        else
            echo "Waiting for Airflow to start... (attempt $attempt/$max_attempts)"
        fi
        
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "⚠️ Airflow taking longer than expected to start"
    return 1
}

# Start PostgreSQL first (without dependencies)
echo "1. Starting PostgreSQL independently..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d --no-deps postgres

# Wait for PostgreSQL to be ready
if wait_for_postgres; then
    echo "✅ PostgreSQL is ready for Airflow"
else
    echo "❌ Cannot start Airflow without PostgreSQL"
    echo "Check PostgreSQL logs: docker compose -f $COMPOSE_FILE logs postgres"
    exit 1
fi

# Start Airflow services independently (bypass dependencies)
echo ""
echo "2. Starting Airflow services independently..."
echo "   Starting airflow-init (database initialization)..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d --no-deps airflow-init

echo "   Waiting for Airflow database initialization..."
sleep 25

echo "   Starting airflow-webserver and airflow-scheduler..."
BUCKET=telecom-data docker compose -f "$COMPOSE_FILE" up -d --no-deps airflow-webserver airflow-scheduler

echo ""
echo "3. Waiting for Airflow platform to initialize (this can take 2-3 minutes)..."
sleep 30

# Check Airflow status
if wait_for_airflow; then
    echo "✅ Airflow platform is fully operational!"
else
    echo "⚠️ Airflow is starting up in the background"
    echo "   It will be available at http://localhost:8083 shortly"
fi

# Final status check
echo ""
echo "=== Airflow Services Status ==="
AIRFLOW_SERVICES=("postgres" "airflow-init" "airflow-webserver" "airflow-scheduler")

for service in "${AIRFLOW_SERVICES[@]}"; do
    if is_service_running "$service"; then
        echo "✅ $service: RUNNING"
    else
        echo "❌ $service: NOT RUNNING"
        echo "   Check logs: docker compose -f $COMPOSE_FILE logs $service"
    fi
done

echo ""
echo "=== Access Information ==="
echo "🌐 Airflow Web UI:  http://localhost:8083"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "🗄️  PostgreSQL:     localhost:5434"
echo "   Database: postgres"
echo "   Username: postgres"
echo "   Password: postgres"
echo ""
echo "=== Next Steps ==="
echo "📊 Access Airflow:    http://localhost:8083"
echo "📋 Check Airflow logs: docker compose -f $COMPOSE_FILE logs airflow-webserver"
echo "🗄️  Check PostgreSQL:  docker compose -f $COMPOSE_FILE logs postgres"
echo "🛑 Stop Airflow:      ./stop-airflow.sh"
