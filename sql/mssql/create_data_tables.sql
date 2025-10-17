-- =============================================
-- Create smart_meter_data table
-- =============================================

USE telecom_db;
GO

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

CREATE INDEX idx_smart_meter_timestamp ON smart_meter_data(timestamp);
CREATE INDEX idx_smart_meter_meter_id ON smart_meter_data(meter_id);
GO

PRINT '✅ smart_meter_data table created with indexes';
GO


-- =============================================
-- Insert sample data
-- =============================================

INSERT INTO smart_meter_data (
    meter_id, timestamp, energy_consumption, voltage, current, power_factor, frequency
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

PRINT '✅ Sample data inserted successfully';
GO


-- =============================================
-- Create additional tables for analytics
-- =============================================

-- Table for meter metadata
CREATE TABLE meter_metadata (
    meter_id NVARCHAR(50) PRIMARY KEY,
    location NVARCHAR(100),
    customer_id NVARCHAR(50),
    meter_type NVARCHAR(50),
    installation_date DATE,
    max_capacity DECIMAL(10,2),
    is_active BIT DEFAULT 1
);
GO

-- Insert meter metadata
INSERT INTO meter_metadata (
    meter_id, location, customer_id, meter_type, installation_date, max_capacity
)
VALUES 
    ('METER_001', 'Downtown Area A', 'CUST_1001', 'Industrial', '2023-01-15', 100.0),
    ('METER_002', 'Residential Zone B', 'CUST_1002', 'Residential', '2023-02-20', 50.0),
    ('METER_003', 'Commercial District C', 'CUST_1003', 'Commercial', '2023-03-10', 75.0),
    ('METER_004', 'Suburban Area D', 'CUST_1004', 'Residential', '2023-01-25', 50.0),
    ('METER_005', 'Industrial Zone E', 'CUST_1005', 'Industrial', '2023-04-05', 150.0);
GO

-- Table for daily aggregations
CREATE TABLE daily_energy_consumption (
    id INT IDENTITY(1,1) PRIMARY KEY,
    meter_id NVARCHAR(50),
    consumption_date DATE,
    total_energy DECIMAL(12,2),
    avg_voltage DECIMAL(10,2),
    avg_current DECIMAL(10,2),
    max_consumption DECIMAL(10,2),
    record_count INT,
    created_at DATETIME2 DEFAULT GETDATE()
);
GO

CREATE INDEX idx_daily_consumption_date ON daily_energy_consumption(consumption_date);
CREATE INDEX idx_daily_consumption_meter ON daily_energy_consumption(meter_id);
GO

PRINT '✅ Additional analytics tables created';
GO
