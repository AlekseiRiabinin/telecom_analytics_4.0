#!/bin/bash

echo "=== Starting Kafka (Single-Node KRaft) ==="

COMPOSE_FILE="docker-compose.telecom.yml"

echo "1. Starting Kafka service..."
docker compose -f "$COMPOSE_FILE" up -d kafka-1

echo ""
echo "2. Waiting for Kafka to become ready..."

# Robust readiness check
for i in {1..20}; do
    if docker exec kafka-1 bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list" >/dev/null 2>&1; then
        echo "Kafka is ready."
        break
    fi
    echo "Kafka not ready yet... retrying ($i/20)"
    sleep 2
done

echo ""
echo "3. Creating Kafka topics..."

docker exec kafka-1 bash -c '
    topics=(
        "rover-telemetry-raw:4"
        "rover-telemetry-enriched:4"
        "rover-analytics:4"
        "cityrover-spark-metrics:1"
    )

    for topic in "${topics[@]}"; do
        IFS=":" read -r name partitions <<< "$topic"

        if /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic "$name" >/dev/null 2>&1; then
            echo "Topic exists: $name"
        else
            echo "Creating topic: $name"
            /opt/kafka/bin/kafka-topics.sh --create \
                --topic "$name" \
                --partitions "$partitions" \
                --replication-factor 1 \
                --bootstrap-server kafka-1:19092
        fi
    done
'

echo ""
echo "4. Kafka topics created successfully."
echo "=== Kafka Status ==="
docker exec kafka-1 bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list"

echo ""
echo "=== Kafka is running and ready ==="
echo "Next steps:"
echo "  - Start Spark: ./start-spark.sh"
echo "  - Start ClickHouse: ./start-clickhouse.sh"
echo "  - Start Airflow: ./start-airflow.sh"
