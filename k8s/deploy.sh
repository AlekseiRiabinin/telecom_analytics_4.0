#!/bin/bash
# k8s/deploy.sh

set -e

echo "🚀 Deploying Spark RBAC to Minikube..."

# Apply RBAC configuration
kubectl apply -f spark-rbac.yaml

echo "✅ RBAC deployed successfully!"

# Wait a moment for resources to be created
sleep 3

# Verify deployment
echo "📋 Checking deployment status..."
kubectl get sa -n spark-jobs
kubectl get rolebindings -n spark-jobs
kubectl get clusterrolebindings | grep spark

echo ""
echo "🎯 Next steps:"
echo "   1. Build and load Docker image: ./docker/build.sh"
echo "   2. Load image to Minikube: minikube image load alexflames77/telecom-analytics:latest"
echo "   3. Submit Spark job: kubectl apply -f spark-job.yaml"
echo "   4. Monitor logs: kubectl logs -f job/telecom-analytics-job -n spark-jobs"
