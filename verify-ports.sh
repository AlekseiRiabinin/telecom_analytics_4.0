#!/bin/bash

echo "=== Verifying Kafka Port Configuration ==="

echo "1. Checking docker-compose ports..."
grep -A5 "kafka-1:" docker-compose.telecom.yml | grep ports
grep -A5 "kafka-2:" docker-compose.telecom.yml | grep ports

echo ""
echo "2. Checking server properties..."
echo "server-1.properties listeners:"
grep "listeners" server-1.properties
grep "controller.quorum.voters" server-1.properties

echo ""
echo "server-2.properties listeners:"
grep "listeners" server-2.properties
grep "controller.quorum.voters" server-2.properties

echo ""
echo "3. Checking Debezium configuration..."
grep "BOOTSTRAP_SERVERS" docker-compose.telecom.yml
grep "bootstrap.servers" debezium/debezium-mssql-connector.json

echo ""
echo "✅ Port verification complete"
