#!/bin/bash

# start-city-rover.sh - Unified startup script for City-Rover platform
# Combines functionality from start-spark.sh, start-airflow.sh, and start-clickhouse-manual.sh

set -e

# Cleanup function for interrupted script
cleanup() {
    print_message "${YELLOW}" ""
    print_message "${YELLOW}" "⚠️ Startup interrupted."
    
    read -p "Stop partially started services? (Y/n): " -r answer
    
    if [[ ! "$answer" =~ ^[Nn]$ ]]; then
        docker compose -f "$COMPOSE_FILE" down
        print_message "${GREEN}" "✅ Services stopped."
    fi
    
    exit 130
}

trap cleanup INT TERM

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.city-rover.yml"
LOG_FILE="${SCRIPT_DIR}/logs/startup-$(date +%Y%m%d-%H%M%S).log"
SKIP_INIT=false
BUILD_FLAG=false

# Create logs directory
mkdir -p "${SCRIPT_DIR}/logs"

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${message}" >> "${LOG_FILE}"
}

# Function to check if service is running
is_service_running() {
    docker compose -f "$COMPOSE_FILE" ps "$1" 2>/dev/null | grep -q "Up"
}

# Function to check if service is healthy
is_service_healthy() {
    docker compose -f "$COMPOSE_FILE" ps "$1" 2>/dev/null | grep -q "healthy"
}

# Function to wait for PostgreSQL
wait_for_postgres() {
    local max_attempts=30
    local attempt=1
    
    print_message "${BLUE}" "⏳ Waiting for PostgreSQL to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if docker compose -f "$COMPOSE_FILE" exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
            print_message "${GREEN}" "✅ PostgreSQL is ready! (attempt $attempt)"
            return 0
        fi
        echo -n "."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    print_message "${RED}" "❌ PostgreSQL not ready after $max_attempts attempts"
    return 1
}

# Function to wait for Redis
wait_for_redis() {
    local max_attempts=20
    local attempt=1
    
    print_message "${BLUE}" "⏳ Waiting for Redis to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if docker compose -f "$COMPOSE_FILE" exec -T redis redis-cli ping 2>/dev/null | grep -q "PONG"; then
            print_message "${GREEN}" "✅ Redis is ready! (attempt $attempt)"
            return 0
        fi
        echo -n "."
        sleep 3
        attempt=$((attempt + 1))
    done
    
    print_message "${RED}" "❌ Redis not ready after $max_attempts attempts"
    return 1
}

# Function to wait for ClickHouse
wait_for_clickhouse() {
    local max_attempts=40
    local attempt=1
    
    print_message "${BLUE}" "⏳ Waiting for ClickHouse to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:8123/ping 2>/dev/null | grep -q "Ok"; then
            print_message "${GREEN}" "✅ ClickHouse is ready! (attempt $attempt)"
            return 0
        fi
        echo -n "."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    print_message "${RED}" "❌ ClickHouse not ready after $max_attempts attempts"
    return 1
}

# Function to wait for Spark
wait_for_spark() {
    local max_attempts=30
    local attempt=1
    
    print_message "${BLUE}" "⏳ Waiting for Spark Master to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:8091 2>/dev/null | grep -q "Spark"; then
            print_message "${GREEN}" "✅ Spark Master is ready! (attempt $attempt)"
            return 0
        fi
        echo -n "."
        sleep 3
        attempt=$((attempt + 1))
    done
    
    print_message "${YELLOW}" "⚠️  Spark Master Web UI not accessible, but services may be starting..."
    return 1
}

# Function to wait for Airflow
wait_for_airflow() {
    local max_attempts=40
    local attempt=1
    
    print_message "${BLUE}" "⏳ Waiting for Airflow webserver to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:8083/health 2>/dev/null | grep -q "healthy"; then
            print_message "${GREEN}" "✅ Airflow webserver is ready! (attempt $attempt)"
            return 0
        fi
        echo -n "."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    print_message "${YELLOW}" "⚠️  Airflow taking longer than expected to start"
    return 1
}

# Function to check Celery workers
check_celery_workers() {
    local max_attempts=20
    local attempt=1
    
    print_message "${BLUE}" "⏳ Checking Celery workers..."
    while [ $attempt -le $max_attempts ]; do
        local worker1_running=$(is_service_running "airflow-worker-1" && echo "yes" || echo "no")
        local worker2_running=$(is_service_running "airflow-worker-2" && echo "yes" || echo "no")
        
        if [ "$worker1_running" = "yes" ] && [ "$worker2_running" = "yes" ]; then
            print_message "${GREEN}" "✅ Both Celery workers are running"
            
            # Check Redis connection
            if docker compose -f "$COMPOSE_FILE" logs airflow-worker-1 2>/dev/null | grep -q "Connected to redis"; then
                print_message "${GREEN}" "✅ Worker 1 connected to Redis"
            fi
            if docker compose -f "$COMPOSE_FILE" logs airflow-worker-2 2>/dev/null | grep -q "Connected to redis"; then
                print_message "${GREEN}" "✅ Worker 2 connected to Redis"
            fi
            return 0
        fi
        echo -n "."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    print_message "${YELLOW}" "⚠️  Workers might still be initializing"
    return 1
}

# Function to check network
check_network() {
    print_message "${BLUE}" "🔍 Checking network..."
    if ! docker network inspect city-rover-net >/dev/null 2>&1; then
        print_message "${YELLOW}" "⚠️  Network 'city-rover-net' does not exist. Creating it..."
        docker network create city-rover-net
        print_message "${GREEN}" "✅ Network created"
    else
        print_message "${GREEN}" "✅ Network exists"
    fi
}

# Function to check port conflicts (improved - handles missing lsof)
check_ports() {
    print_message "${BLUE}" "🔍 Checking port availability..."
    
    local ports=(5434 6379 8123 9000 7077 8091 8083)
    local conflicts=0
    
    # Check if netstat is available (more commonly installed than lsof)
    if command -v netstat >/dev/null 2>&1; then
        for port in "${ports[@]}"; do
            if netstat -tulpn 2>/dev/null | grep -q ":$port "; then
                print_message "${YELLOW}" "⚠️  Port ${port} is in use"
                conflicts=$((conflicts+1))
            fi
        done
    # Fallback to ss command (available on modern Linux)
    elif command -v ss >/dev/null 2>&1; then
        for port in "${ports[@]}"; do
            if ss -tulpn 2>/dev/null | grep -q ":$port "; then
                print_message "${YELLOW}" "⚠️  Port ${port} is in use"
                conflicts=$((conflicts+1))
            fi
        done
    # Fallback to lsof if available
    elif command -v lsof >/dev/null 2>&1; then
        for port in "${ports[@]}"; do
            if lsof -Pi :${port} -sTCP:LISTEN -t >/dev/null 2>&1; then
                print_message "${YELLOW}" "⚠️  Port ${port} is in use"
                conflicts=$((conflicts+1))
            fi
        done
    else
        print_message "${YELLOW}" "⚠️  Could not check ports (no netstat/ss/lsof found)"
        print_message "${YELLOW}" "    Continuing anyway..."
        return 0
    fi
    
    if [ $conflicts -eq 0 ]; then
        print_message "${GREEN}" "✅ All ports available"
    else
        print_message "${YELLOW}" "⚠️  ${conflicts} port(s) in use. Services may fail to start."
    fi
}

# Function to show status
show_status() {
    echo ""
    print_message "${BLUE}" "=========================================="
    print_message "${BLUE}" "📊 Service Status"
    print_message "${BLUE}" "=========================================="
    
    local services=(
        "postgres"
        "redis"
        "clickhouse"
        "spark-master"
        "spark-worker"
        "airflow-webserver"
        "airflow-scheduler"
        "airflow-worker-1"
        "airflow-worker-2"
    )
    
    for service in "${services[@]}"; do
        if is_service_running "$service"; then
            if is_service_healthy "$service" 2>/dev/null; then
                print_message "${GREEN}" "✅ $service: RUNNING (healthy)"
            else
                print_message "${GREEN}" "✅ $service: RUNNING"
            fi
        else
            print_message "${RED}" "❌ $service: NOT RUNNING"
        fi
    done
}

# Main startup function
start_city_rover() {
    print_message "${BLUE}" "=========================================="
    print_message "${BLUE}" "🚀 Starting City-Rover Platform"
    print_message "${BLUE}" "=========================================="
    print_message "" "Log file: ${LOG_FILE}"
    
    # Pre-flight checks
    check_network
    check_ports
    
    # Check if compose file exists
    if [ ! -f "${COMPOSE_FILE}" ]; then
        print_message "${RED}" "❌ Docker Compose file not found: ${COMPOSE_FILE}"
        exit 1
    fi
    
    # Check Docker
    if ! docker info >/dev/null 2>&1; then
        print_message "${RED}" "❌ Docker is not running or not accessible"
        exit 1
    fi
    
    # Build if requested
    if [ "$BUILD_FLAG" = true ]; then
        print_message "${BLUE}" "🔨 Building Docker images..."
        docker compose -f "$COMPOSE_FILE" build --no-cache
        print_message "${GREEN}" "✅ Images built"
    fi
    
    # Check if services are already running
    print_message "${BLUE}" "🔍 Checking existing services..."
    if is_service_running "spark-master" || is_service_running "airflow-webserver"; then
        print_message "${YELLOW}" "⚠️  Some services appear to be running."
        read -p "Restart all services? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_message "${BLUE}" "Exiting without changes."
            exit 0
        fi
        print_message "${BLUE}" "🛑 Stopping existing services..."
        docker compose -f "$COMPOSE_FILE" down
        sleep 5
    fi
    
    # Phase 1: Start core infrastructure
    print_message "${BLUE}" ""
    print_message "${BLUE}" "📊 Phase 1: Starting Core Infrastructure..."
    
    print_message "${BLUE}" "   Starting PostgreSQL..."
    docker compose -f "$COMPOSE_FILE" up -d postgres
    wait_for_postgres || exit 1
    
    print_message "${BLUE}" "   Starting Redis..."
    docker compose -f "$COMPOSE_FILE" up -d redis
    wait_for_redis || exit 1
    
    # Phase 2: Start ClickHouse
    print_message "${BLUE}" ""
    print_message "${BLUE}" "📊 Phase 2: Starting ClickHouse..."
    print_message "${BLUE}" "   Starting ClickHouse..."
    docker compose -f "$COMPOSE_FILE" up -d --no-deps clickhouse
    wait_for_clickhouse || exit 1

    # Phase 3: Copy Spark JARs into shared directory
    print_message "${BLUE}" ""
    print_message "${BLUE}" "📦 Phase 3: Preparing Spark JARs..."

    SHARED_JAR_DIR="../spatio_temporal_stream_processing/batch-processing/city-rover/spark-jars"
    JOB_JAR="../spatio_temporal_stream_processing/batch-processing/city-rover/spark-jobs/trajectory-visualizer-job/target/scala-2.12/cityrover-trajectory-visualizer-job-assembly-0.1.0.jar"

    if [ -f "$JOB_JAR" ]; then
        print_message "${BLUE}" "   Copying trajectory-visualizer-job JAR..."
        cp "$JOB_JAR" "$SHARED_JAR_DIR/"
        print_message "${GREEN}" "   JAR copied to shared directory."
    else
        print_message "${RED}" "❌ JAR not found: $JOB_JAR"
        print_message "${YELLOW}" "   Run 'sbt assembly' in trajectory-visualizer-job first."
        exit 1
    fi

    # Phase 4: Start Spark
    print_message "${BLUE}" ""
    print_message "${BLUE}" "⚡ Phase 4: Starting Spark Cluster..."
    print_message "${BLUE}" "   Starting Spark Master and Worker..."
    docker compose -f "$COMPOSE_FILE" up -d spark-master spark-worker
    wait_for_spark
    
    # Phase 5: Start Airflow
    print_message "${BLUE}" ""
    print_message "${BLUE}" "🌐 Phase 5: Starting Airflow Platform..."
    
    if [ "$SKIP_INIT" = false ]; then
        print_message "${BLUE}" "   Initializing Airflow database..."
        
        # Run airflow-init and wait for completion
        if docker compose -f "$COMPOSE_FILE" up airflow-init; then
            print_message "${GREEN}" "✅ Airflow initialization completed"
        else
            print_message "${RED}" "❌ Airflow initialization failed"
            print_message "${YELLOW}" "Check logs: docker compose -f $COMPOSE_FILE logs airflow-init"
            exit 1
        fi
    else
        print_message "${YELLOW}" "⚠️  Skipping Airflow initialization (--no-init used)"
    fi
    
    print_message "${BLUE}" "   Starting Airflow services..."
    docker compose -f "$COMPOSE_FILE" up -d --no-deps \
        airflow-webserver \
        airflow-scheduler \
        airflow-worker-1 \
        airflow-worker-2
    
    print_message "${BLUE}" "   Waiting for Airflow to initialize..."
    wait_for_airflow
    check_celery_workers
    
    # Show status
    show_status
    
    # Show access information
    print_message "${BLUE}" ""
    print_message "${BLUE}" "=========================================="
    print_message "${GREEN}" "✅ City-Rover Platform Started Successfully!"
    print_message "${BLUE}" "=========================================="
    
    print_message "${BLUE}" ""
    print_message "${BLUE}" "🌐 Service Access URLs:"
    print_message "${GREEN}" "  Airflow Web UI:     http://localhost:8083"
    print_message "${GREEN}" "  Spark Master UI:    http://localhost:8091"
    print_message "${GREEN}" "  ClickHouse HTTP:    http://localhost:8123"
    print_message "${GREEN}" "  ClickHouse Native:  localhost:9000"
    print_message "${GREEN}" "  Redis:              localhost:6379"
    print_message "${GREEN}" "  PostgreSQL:         localhost:5434"
    
    print_message "${BLUE}" ""
    print_message "${BLUE}" "📝 Credentials:"
    print_message "${GREEN}" "  Airflow:     admin / admin"
    print_message "${GREEN}" "  ClickHouse:  admin / clickhouse_admin"
    print_message "${GREEN}" "  PostgreSQL:  postgres / postgres"
    
    print_message "${BLUE}" ""
    print_message "${BLUE}" "📋 Useful Commands:"
    print_message "${YELLOW}" "  View all logs:     docker compose -f ${COMPOSE_FILE} logs -f"
    print_message "${YELLOW}" "  View service logs: docker compose -f ${COMPOSE_FILE} logs -f [service]"
    print_message "${YELLOW}" "  Stop all:          docker compose -f ${COMPOSE_FILE} down"
    print_message "${YELLOW}" "  Check status:      ./status-city-rover.sh"
    print_message "${YELLOW}" "  View resources:    docker stats"
    
    print_message "${BLUE}" ""
    print_message "${BLUE}" "📄 Log file: ${LOG_FILE}"
    print_message "${GREEN}" "🎉 City-Rover platform is now running!"
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --help|-h)
                echo "Usage: ./start-city-rover.sh [options]"
                echo ""
                echo "Options:"
                echo "  --build     Rebuild images before starting"
                echo "  --no-init   Skip Airflow initialization"
                echo "  --help      Show this help message"
                exit 0
                ;;
            --build)
                BUILD_FLAG=true
                shift
                ;;
            --no-init)
                SKIP_INIT=true
                print_message "${YELLOW}" "⚠️  Airflow initialization will be skipped"
                shift
                ;;
            *)
                print_message "${RED}" "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Run the main function
    start_city_rover
fi
