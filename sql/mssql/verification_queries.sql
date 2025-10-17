-- Check all databases
SELECT name FROM sys.databases WHERE name IN ('telecom_db', 'airflow_db', 'spark_db');
GO

-- Check tables in telecom_db
USE telecom_db;
SELECT name FROM sys.tables;
GO

-- Check sample data
SELECT 
    meter_id,
    COUNT(*) as record_count,
    AVG(energy_consumption) as avg_consumption,
    MIN(timestamp) as first_reading,
    MAX(timestamp) as last_reading
FROM smart_meter_data 
GROUP BY meter_id;
GO

PRINT '🎉 MSSQL Telecom Analytics Database Setup Complete!';
GO
