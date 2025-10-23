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

# Function to wait for Redis to be ready
wait_for_redis() {
    local max_attempts=20
    local attempt=1
    
    echo "Waiting for Redis to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if docker compose -f "$COMPOSE_FILE" exec redis redis-cli ping | grep -q "PONG"; then
            echo "✅ Redis is ready!"
            return 0
        fi
        echo "Waiting for Redis... (attempt $attempt/$max_attempts)"
        sleep 3
        attempt=$((attempt + 1))
    done
    
    echo "❌ Redis not ready after $max_attempts attempts"
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

# Function to check Celery workers status
check_celery_workers() {
    local max_attempts=20
    local attempt=1
    
    echo "Checking Celery workers status..."
    while [ $attempt -le $max_attempts ]; do
        local worker1_running=$(is_service_running "airflow-worker-1" && echo "yes" || echo "no")
        local worker2_running=$(is_service_running "airflow-worker-2" && echo "yes" || echo "no")
        
        if [ "$worker1_running" = "yes" ] && [ "$worker2_running" = "yes" ]; then
            echo "✅ Both Celery worker containers are running and healthy"
            
            # Quick test to see if workers are responsive
            if docker compose -f "$COMPOSE_FILE" logs airflow-worker-1 2>/dev/null | grep -q "Connected to redis"; then
                echo "✅ Worker 1 connected to Redis successfully"
            fi
            
            if docker compose -f "$COMPOSE_FILE" logs airflow-worker-2 2>/dev/null | grep -q "Connected to redis"; then
                echo "✅ Worker 2 connected to Redis successfully"
            fi
            
            echo "Workers are starting up and will register with Airflow shortly"
            return 0
        else
            echo "Waiting for worker containers... (attempt $attempt/$max_attempts)"
            echo "  Worker 1: $worker1_running, Worker 2: $worker2_running"
        fi
        
        sleep 5
        attempt=$((attempt + 1))
    done
    
    echo "❌ Worker containers failed to start within timeout"
    return 1
}

# Start core services first
echo "1. Starting core services (PostgreSQL & Redis)..."
docker compose -f "$COMPOSE_FILE" up -d --no-deps postgres redis

# Wait for PostgreSQL to be ready
if wait_for_postgres; then
    echo "✅ PostgreSQL is ready for Airflow"
else
    echo "❌ Cannot start Airflow without PostgreSQL"
    echo "Check PostgreSQL logs: docker compose -f $COMPOSE_FILE logs postgres"
    exit 1
fi

# Wait for Redis to be ready
if wait_for_redis; then
    echo "✅ Redis is ready for Celery"
else
    echo "❌ Cannot start Airflow workers without Redis"
    echo "Check Redis logs: docker compose -f $COMPOSE_FILE logs redis"
    exit 1
fi

# Start Airflow initialization
echo ""
echo "2. Starting Airflow services independently..."
echo "   Starting airflow-init (database initialization)..."
docker compose -f "$COMPOSE_FILE" up -d --no-deps airflow-init

echo "   Waiting for Airflow database initialization..."
sleep 25

# Start main Airflow services

echo ""
echo "3. Starting main Airflow services..."
echo "   Starting airflow-webserver, airflow-scheduler, and airflow-worker..."
docker compose -f "$COMPOSE_FILE" up -d --no-deps airflow-webserver airflow-scheduler airflow-worker-1 airflow-worker-2

echo ""
echo "4. Waiting for Airflow platform to initialize (this can take 2-3 minutes)..."
sleep 30

# Check Airflow status
if wait_for_airflow; then
    echo "✅ Airflow platform is fully operational!"
else
    echo "⚠️ Airflow is starting up in the background"
    echo "   It will be available at http://localhost:8083 shortly"
fi

# Check Celery workers
echo ""
echo "5. Checking Celery workers..."
if check_celery_workers; then
    echo "✅ Celery executor setup is working correctly"
else
    echo "⚠️ Workers might still be initializing"
    echo "   You can check worker status in Airflow UI -> Workers"
fi

# Final status check
echo ""
echo "=== Airflow Services Status ==="
AIRFLOW_SERVICES=(
    "postgres" "redis" "airflow-init" 
    "airflow-webserver" "airflow-scheduler" 
    "airflow-worker-1" "airflow-worker-2"
)

for service in "${AIRFLOW_SERVICES[@]}"; do
    if is_service_running "$service"; then
        echo "✅ $service: RUNNING"
    else
        echo "❌ $service: NOT RUNNING"
        # Don't show log command for airflow-init (it's expected to exit)
        if [ "$service" != "airflow-init" ]; then
            echo "   Check logs: docker compose -f $COMPOSE_FILE logs $service"
        fi
    fi
done

# Show worker details
echo ""
echo "=== Worker Details ==="
WORKER1_STATUS=$(is_service_running "airflow-worker-1" && echo "RUNNING" || echo "STOPPED")
WORKER2_STATUS=$(is_service_running "airflow-worker-2" && echo "RUNNING" || echo "STOPPED")
echo "👥 Worker 1: $WORKER1_STATUS"
echo "👥 Worker 2: $WORKER2_STATUS"

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
echo "🔴 Redis:           localhost:6379"
echo "   Used for Celery task queue"
echo ""
echo "=== Next Steps ==="
echo "📊 Access Airflow:    http://localhost:8083"
echo "👥 Check workers:     Airflow UI -> Workers tab"
echo "📋 Check Airflow logs: docker compose -f $COMPOSE_FILE logs airflow-webserver"
echo "🔍 Check worker 1 logs: docker compose -f $COMPOSE_FILE logs airflow-worker-1"
echo "🔍 Check worker 2 logs: docker compose -f $COMPOSE_FILE logs airflow-worker-2"
echo "🗄️  Check PostgreSQL:  docker compose -f $COMPOSE_FILE logs postgres"
echo "🔴 Check Redis:       docker compose -f $COMPOSE_FILE logs redis"
echo "🛑 Stop Airflow:      ./stop-airflow.sh"

echo ""
echo "=== Important Notes ==="
echo "💡 Using CeleryExecutor for parallel task processing"
echo "💡 2 dedicated worker instances for load distribution"
echo "💡 Redis is used as message broker for task queue"
echo "💡 PostgreSQL is used for metadata and result backend"
echo "💡 Each worker has separate container for better isolation"
