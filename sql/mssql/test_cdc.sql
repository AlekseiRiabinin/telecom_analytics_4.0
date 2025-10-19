-- First, check current data
SELECT COUNT(*) AS current_count FROM smart_meter_data;
SELECT TOP 5 * FROM smart_meter_data ORDER BY id DESC;
GO

-- Generate some test changes using your procedures
EXEC GenerateNewMeterReadings 3;
EXEC UpdateMeterReadings 2;
EXEC DeleteMeterReadings 1;
GO

-- Wait a few seconds for CDC to capture changes
WAITFOR DELAY '00:00:05';
GO
