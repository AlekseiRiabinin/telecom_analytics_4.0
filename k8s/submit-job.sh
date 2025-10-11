#!/bin/bash
# k8s/submit-job.sh

set -e

MINIKUBE_IP=$(minikube ip)

echo "🚀 Submitting Spark job to Minikube..."

spark-submit \
  --master k8s://https://${MINIKUBE_IP}:8443 \
  --deploy-mode cluster \
  --name telecom-analytics \
  --class com.telecomanalytics.CorePipeline \
  --conf spark.kubernetes.container.image=alexflames77/telecom-analytics:latest \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.executor.instances=2 \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.sql.adaptive.enabled=true \
  local:///opt/spark/work-dir/app.jar

echo "✅ Job submitted successfully!"
echo "📋 Monitor with: kubectl get pods -n spark-jobs -w"
