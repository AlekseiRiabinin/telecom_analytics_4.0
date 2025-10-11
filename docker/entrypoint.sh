#!/bin/bash
# docker/entrypoint.sh

set -e  # Exit on any error

echo "🚀 Starting Telecom Analytics Spark Job"
echo "📍 Spark Version: 4.0.1"
echo "📍 Scala Version: 2.13.17"
echo "📍 Java Version: $(java -version 2>&1 | head -n1)"

# Wait for HDFS to be ready (if needed in your environment)
echo "⏳ Checking HDFS availability..."
until curl -f http://hdfs-namenode:9870/ > /dev/null 2>&1; do
    echo "📡 Waiting for HDFS namenode..."
    sleep 10
done

# Wait for ClickHouse to be ready
echo "⏳ Checking ClickHouse availability..."
until curl -f http://clickhouse:8123/ > /dev/null 2>&1; do
    echo "🗄️ Waiting for ClickHouse..."
    sleep 5
done

echo "✅ All dependencies ready!"

# Submit Spark job with optimized configurations
exec /opt/spark/bin/spark-submit \
    --class com.telecomanalytics.CorePipeline \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.sql.legacy.timeParserPolicy=LEGACY \
    /opt/spark/work-dir/app.jar "$@"
