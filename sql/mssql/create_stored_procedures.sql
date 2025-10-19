USE telecom_db;
GO

-- =============================================
-- Stored Procedures to Generate DML Changes
-- =============================================

-- Procedure to insert new meter readings
CREATE OR ALTER PROCEDURE GenerateNewMeterReadings
    @NumberOfRecords INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Counter INT = 1;
    
    WHILE @Counter <= @NumberOfRecords
    BEGIN
        INSERT INTO smart_meter_data (
            meter_id, 
            timestamp, 
            energy_consumption, 
            voltage, 
            current_reading, 
            power_factor, 
            frequency
        )
        VALUES (
            'METER_' + RIGHT('000' + CAST((ABS(CHECKSUM(NEWID())) % 20) + 1 AS NVARCHAR(3)), 3),
            DATEADD(MINUTE, - (ABS(CHECKSUM(NEWID())) % 1440), GETDATE()),
            ROUND((RAND() * 30) + 5, 2),  -- 5-35 kWh
            ROUND(220 + (RAND() * 5), 1),  -- 220-225V
            ROUND((RAND() * 15) + 1, 1),   -- 1-16A
            ROUND(0.9 + (RAND() * 0.1), 2), -- 0.9-1.0
            ROUND(49.8 + (RAND() * 0.4), 1) -- 49.8-50.2 Hz
        );
        
        SET @Counter = @Counter + 1;
    END;
    
    PRINT '✅ Generated ' + CAST(@NumberOfRecords AS NVARCHAR) + ' new meter readings';
END;
GO

-- Procedure to update existing records
CREATE OR ALTER PROCEDURE UpdateMeterReadings
    @NumberOfUpdates INT = 5
AS
BEGIN
    SET NOCOUNT ON;
    
    WITH RandomRecords AS (
        SELECT TOP (@NumberOfUpdates) *
        FROM smart_meter_data 
        ORDER BY NEWID()
    )
    UPDATE smart_meter_data
    SET 
        energy_consumption = ROUND(energy_consumption * (0.95 + (RAND() * 0.1)), 2),
        voltage = ROUND(voltage * (0.99 + (RAND() * 0.02)), 1),
        current_reading = ROUND(current_reading * (0.95 + (RAND() * 0.1)), 1),
        updated_at = GETDATE()
    WHERE id IN (SELECT id FROM RandomRecords);
    
    PRINT '✅ Updated ' + CAST(@NumberOfUpdates AS NVARCHAR(10)) + ' meter readings';
END;
GO

-- Procedure to delete some records (FIXED with CTE and ROW_NUMBER)
CREATE OR ALTER PROCEDURE DeleteMeterReadings
    @NumberOfDeletes INT = 3
AS
BEGIN
    SET NOCOUNT ON;
    
    WITH RandomRecords AS (
        SELECT id, ROW_NUMBER() OVER (ORDER BY NEWID()) as rn
        FROM smart_meter_data
    )
    DELETE FROM smart_meter_data
    WHERE id IN (
        SELECT id 
        FROM RandomRecords 
        WHERE rn <= @NumberOfDeletes
    );
    
    PRINT '✅ Deleted ' + CAST(@NumberOfDeletes AS NVARCHAR(10)) + ' meter readings';
END;
GO

-- Procedure to simulate real-time data changes (with BEGIN/END)
CREATE OR ALTER PROCEDURE SimulateRealTimeChanges
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RandomNumber INT = ABS(CHECKSUM(NEWID())) % 3;
    
    IF @RandomNumber = 0
    BEGIN
        EXEC GenerateNewMeterReadings @NumberOfRecords = 3;
    END
    ELSE IF @RandomNumber = 1
    BEGIN
        EXEC UpdateMeterReadings @NumberOfUpdates = 2;
    END
    ELSE 
    BEGIN
        EXEC DeleteMeterReadings @NumberOfDeletes = 1;
    END
END;
GO

PRINT '✅ DML procedures created successfully';
GO
