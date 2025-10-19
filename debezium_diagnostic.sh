#!/bin/bash

# echo "=== Debezium MSSQL Connector Diagnostic ==="

# echo "1. Testing MSSQL database accessibility..."
# docker exec mssql-server /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "Admin123!" -Q "
# SELECT 
#     @@SERVERNAME as server_name,
#     DB_NAME() as current_db,
#     SYSTEM_USER as current_user;
# GO
# "

# echo "2. Checking telecom_db status..."
# docker exec mssql-server /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "Admin123!" -Q "
# IF DB_ID('telecom_db') IS NOT NULL
# BEGIN
#     USE telecom_db;
#     SELECT 
#         'telecom_db exists' as status,
#         (SELECT COUNT(*) FROM sys.tables WHERE name = 'smart_meter_data') as table_exists,
#         (SELECT COUNT(*) FROM smart_meter_data) as row_count;
# END
# ELSE
#     SELECT 'telecom_db does not exist' as status;
# GO
# "

# echo "3. Checking CDC status..."
# docker exec mssql-server /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "Admin123!" -Q "
# USE telecom_db;
# SELECT 
#     name as database_name,
#     is_cdc_enabled
# FROM sys.databases 
# WHERE name = 'telecom_db';

# SELECT 
#     name as table_name,
#     is_tracked_by_cdc
# FROM sys.tables 
# WHERE name = 'smart_meter_data';
# GO
# "

# echo "4. Current connectors..."
# curl -s http://localhost:8087/connectors | jq .

# echo "=== Diagnostic Complete ==="


# Monitor Debezium logs while creating the connector
docker compose -f docker-compose.telecom.yml logs -f debezium-connect &
LOG_PID=$!

# Create the connector and see what happens
curl -v -X POST -H "Content-Type: application/json" \
  --data @debezium-mssql-connector.json \
  http://localhost:8087/connectors

# Wait and check status
sleep 10
curl -s http://localhost:8087/connectors/mssql-telecom-connector/status

# Stop log monitoring
kill $LOG_PID
