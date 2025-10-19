USE telecom_db;
GO

-- Enable CDC at database level
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC for smart_meter_data table
EXEC sys.sp_cdc_enable_table  
    @source_schema = N'dbo',  
    @source_name   = N'smart_meter_data',  
    @role_name     = NULL,  
    @capture_instance = N'dbo_smart_meter_data',
    @supports_net_changes = 1;
GO

-- Verify CDC is enabled
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'telecom_db';
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = 'smart_meter_data';
