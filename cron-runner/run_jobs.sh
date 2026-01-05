#!/bin/bash
set -e

DB_HOST="postgis-cre"
DB_NAME="cre_db"
DB_USER="cre_user"

run_sql() {
    local sql="$1"
    psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "$sql"
}

echo "=== Running scheduled CRE jobs ==="

run_sql "SELECT jobs.run_refresh_node_cache();"
run_sql "SELECT jobs.run_refresh_building_metrics();"
run_sql "SELECT jobs.run_refresh_ml_features();"
run_sql "SELECT jobs.run_refresh_materialized_views();"
run_sql "SELECT jobs.run_cleanup_job_logs();"

echo "=== Jobs completed ==="
