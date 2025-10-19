#!/bin/bash

echo "=== Creating Debezium MSSQL Connector ==="

# Delete any existing connector
echo "1. Cleaning up existing connector..."
curl -s -X DELETE http://localhost:8087/connectors/mssql-telecom-connector
sleep 2

# Create the connector
echo "2. Creating new connector..."
response=$(curl -m 60 -s -w "\n%{http_code}" -X POST -H "Content-Type: application/json" \
  --data @debezium-mssql-connector.json \
  http://localhost:8087/connectors)

http_code=$(echo "$response" | tail -1)
response_body=$(echo "$response" | head -n -1)

echo "HTTP Status: $http_code"
echo "Response: $response_body"

if [ "$http_code" -eq 201 ]; then
    echo "✅ Connector created successfully!"
    
    echo "3. Waiting for connector to initialize..."
    sleep 10
    
    echo "4. Checking connector status..."
    curl -s http://localhost:8087/connectors/mssql-telecom-connector/status | jq .
    
    echo "5. Testing CDC data flow..."
    echo "Waiting a moment for snapshot to start..."
    sleep 5
    
    # Check for CDC messages
    docker exec kafka-1 /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka-1:9092 \
      --topic telecom-cdc.dbo.smart_meter_data \
      --from-beginning \
      --max-messages 2 \
      --timeout-ms 10000 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "✅ CDC data is flowing!"
    else
        echo "⚠️  No CDC messages yet - connector might be initializing"
    fi
    
else
    echo "❌ Connector creation failed"
    echo "Checking Debezium logs..."
    docker compose -f docker-compose.telecom.yml logs debezium-connect --tail=20 | grep -i error
fi

echo "=== Complete ==="
