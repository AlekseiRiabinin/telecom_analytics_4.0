USE telecom_db;
GO

-- Check if CDC is enabled at database level
SELECT name, is_cdc_enabled 
FROM sys.databases 
WHERE name = 'telecom_db';
GO

-- Check if CDC is enabled for the table
SELECT 
    s.name AS schema_name, 
    t.name AS table_name, 
    t.is_tracked_by_cdc
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name = 'smart_meter_data';
GO

-- Check CDC capture instances
SELECT 
    capture_instance,
    object_id,
    source_object_id,
    start_lsn,
    create_date,
    supports_net_changes
FROM cdc.change_tables;
GO

-- Check CDC jobs (make sure they're running)
SELECT 
    name,
    enabled,
    date_created,
    date_modified
FROM msdb.dbo.sysjobs 
WHERE name LIKE '%cdc%';
GO
