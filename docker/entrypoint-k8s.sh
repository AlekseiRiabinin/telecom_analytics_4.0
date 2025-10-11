#!/bin/bash
# docker/entrypoint-k8s.sh

set -e

echo "🚀 Starting Spark Job on Kubernetes"
echo "📍 Namespace: $(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace 2>/dev/null || echo 'default')"

# Kubernetes-specific configurations
exec /opt/spark/bin/spark-submit \
    --class com.telecomanalytics.CorePipeline \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.sql.legacy.timeParserPolicy=LEGACY \
    /opt/spark/work-dir/app.jar "$@"
