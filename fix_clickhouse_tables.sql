-- Drop all related tables and views
DROP TABLE IF EXISTS cdc_smart_meter_data_mv;
DROP TABLE IF EXISTS cdc_smart_meter_data_kafka;
DROP TABLE IF EXISTS cdc_smart_meter_data_raw;

-- Recreate with simpler structure first
CREATE TABLE cdc_smart_meter_data_raw
(
    `id` Int32,
    `meter_id` String,
    `timestamp` DateTime64(3),
    `energy_consumption` Decimal64(2),
    `voltage` Decimal64(2),
    `current_reading` Decimal64(2),
    `status` String,
    `_cdc_timestamp` DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
ORDER BY (meter_id, timestamp, id)
SETTINGS index_granularity = 8192;

-- Insert test data
INSERT INTO cdc_smart_meter_data_raw 
(id, meter_id, timestamp, energy_consumption, voltage, current_reading, status)
VALUES 
(1, 'meter_001', now(), 15.5, 220.0, 10.2, 'active'),
(2, 'meter_002', now(), 18.3, 219.5, 11.1, 'active');

-- Verify it works
SELECT 'Command line test successful -', count() as row_count FROM cdc_smart_meter_data_raw;
