-- Check if database exists, create if not
IF NOT EXISTS(SELECT name FROM sys.databases WHERE name = 'telecom_db')
BEGIN
    CREATE DATABASE telecom_db;
    PRINT '✅ telecom_db created';
END
ELSE
BEGIN
    PRINT '✅ telecom_db already exists';
END
GO

USE telecom_db;
GO

-- Drop table if exists (for clean setup)
IF OBJECT_ID('smart_meter_data', 'U') IS NOT NULL
BEGIN
    DROP TABLE smart_meter_data;
    PRINT '✅ Existing smart_meter_data dropped';
END
GO

-- Create smart_meter_data table
CREATE TABLE smart_meter_data (
    id INT IDENTITY(1,1) PRIMARY KEY,
    meter_id NVARCHAR(50) NOT NULL,
    timestamp DATETIME2 NOT NULL,
    energy_consumption DECIMAL(10,2) NOT NULL,
    voltage DECIMAL(10,2) NOT NULL,
    current_reading DECIMAL(10,2) NOT NULL,
    power_factor DECIMAL(5,3) NULL,
    frequency DECIMAL(5,2) NULL,
    status NVARCHAR(20) DEFAULT 'active',
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);
GO

PRINT '✅ smart_meter_data table created';
GO

-- Create indexes
CREATE INDEX idx_smart_meter_timestamp ON smart_meter_data(timestamp);
CREATE INDEX idx_smart_meter_meter_id ON smart_meter_data(meter_id);
GO

PRINT '✅ Indexes created';
GO

-- Insert sample data
INSERT INTO smart_meter_data (
    meter_id, timestamp, energy_consumption, voltage, current_reading, power_factor, frequency
)
VALUES 
    ('METER_001', GETDATE(), 15.75, 220.5, 7.1, 0.95, 50.0),
    ('METER_002', GETDATE(), 22.30, 219.8, 10.2, 0.92, 49.9),
    ('METER_003', GETDATE(), 18.45, 221.2, 8.3, 0.98, 50.1),
    ('METER_004', DATEADD(HOUR, -1, GETDATE()), 12.80, 220.1, 5.8, 0.94, 50.0),
    ('METER_005', DATEADD(HOUR, -2, GETDATE()), 25.60, 219.5, 11.6, 0.91, 49.8),
    ('METER_001', DATEADD(HOUR, -3, GETDATE()), 14.20, 220.8, 6.4, 0.96, 50.1),
    ('METER_002', DATEADD(HOUR, -4, GETDATE()), 20.15, 219.9, 9.1, 0.93, 49.9);
GO

PRINT '✅ Sample data inserted';
GO

-- Verify data
SELECT 
    'Verification' as check_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT meter_id) as unique_meters
FROM smart_meter_data;
GO
