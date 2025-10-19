-- Check all databases
SELECT name FROM sys.databases WHERE name IN ('telecom_db', 'airflow_db', 'spark_db');
GO

-- Check tables in telecom_db
USE telecom_db;
SELECT name FROM sys.tables;
GO

-- Check database CDC
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'telecom_db';

-- Check table CDC tracking
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = 'smart_meter_data';

-- Check CDC capture instances (THIS IS IMPORTANT)
SELECT 
    capture_instance,
    object_id, 
    source_schema,
    source_table,
    start_lsn,
    create_date
FROM cdc.change_tables;

-- Check CDC jobs (to ensure they're running)
SELECT 
    name,
    enabled,
    date_created,
    date_modified
FROM msdb.dbo.sysjobs 
WHERE name LIKE '%cdc%';

-- Check if SQL Server Agent is running
EXEC xp_servicecontrol 'QueryState', 'SQLServerAgent';