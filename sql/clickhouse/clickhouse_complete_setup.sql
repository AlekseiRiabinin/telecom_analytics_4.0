-- =============================================
-- RAW DATA TABLES (MergeTree)
-- =============================================

-- Table for CDC data from MSSQL (Debezium)
CREATE TABLE IF NOT EXISTS cdc_smart_meter_data_raw
(
    `id` Int32,
    `meter_id` String,
    `timestamp` DateTime64(9),
    `energy_consumption` Decimal64(2),
    `voltage` Decimal64(2),
    `current_reading` Decimal64(2),
    `power_factor` Nullable(Decimal64(3)),
    `frequency` Nullable(Decimal64(2)),
    `status` String,
    `created_at` DateTime64(9),
    `updated_at` DateTime64(9),
    `_cdc_operation` String,
    `_cdc_timestamp` DateTime64(3) DEFAULT now64(),
    `_cdc_source` String DEFAULT 'mssql_cdc'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (meter_id, timestamp, id)
SETTINGS index_granularity = 8192;

-- Table for direct Kafka producer data
CREATE TABLE IF NOT EXISTS kafka_smart_meter_data_raw
(
    `meter_id` String,
    `timestamp` DateTime64(6),
    `kwh_usage` Decimal64(2),
    `voltage` Int32,
    `customer_id` String,
    `region` String,
    `_ingestion_timestamp` DateTime64(3) DEFAULT now64(),
    `_source` String DEFAULT 'kafka_producer'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (meter_id, timestamp, region)
SETTINGS index_granularity = 8192;

-- =============================================
-- KAFKA ENGINE TABLES (Real-time ingestion)
-- =============================================

-- First drop existing Kafka tables (they can't use IF NOT EXISTS)
DROP TABLE IF EXISTS cdc_smart_meter_data_kafka;
DROP TABLE IF EXISTS kafka_smart_meter_data_kafka;

-- Kafka engine for CDC data
CREATE TABLE cdc_smart_meter_data_kafka
(
    `payload` String
)
ENGINE = Kafka(
    'kafka-1:19092,kafka-2:19094',
    'telecom-cdc.telecom_db.dbo.smart_meter_data',
    'clickhouse-cdc-consumer',
    'JSONAsString'
);

-- Kafka engine for direct producer data
CREATE TABLE kafka_smart_meter_data_kafka
(
    `payload` String
)
ENGINE = Kafka(
    'kafka-1:19092,kafka-2:19094',
    'smart_meter_data',
    'clickhouse-kafka-consumer', 
    'JSONAsString'
);

-- =============================================
-- MATERIALIZED VIEWS FOR DATA TRANSFORMATION
-- =============================================

-- Materialized view for CDC data transformation
CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_smart_meter_data_mv
TO cdc_smart_meter_data_raw
AS
SELECT
    JSONExtractInt(payload, 'after', 'id') as id,
    JSONExtractString(payload, 'after', 'meter_id') as meter_id,
    fromUnixTimestamp64Nano(JSONExtractInt(payload, 'after', 'timestamp')) as timestamp,
    toDecimal64(JSONExtractString(payload, 'after', 'energy_consumption'), 2) as energy_consumption,
    toDecimal64(JSONExtractString(payload, 'after', 'voltage'), 2) as voltage,
    toDecimal64(JSONExtractString(payload, 'after', 'current_reading'), 2) as current_reading,
    if(JSONExtractString(payload, 'after', 'power_factor') != '', 
       toDecimal64(JSONExtractString(payload, 'after', 'power_factor'), 3), NULL) as power_factor,
    if(JSONExtractString(payload, 'after', 'frequency') != '', 
       toDecimal64(JSONExtractString(payload, 'after', 'frequency'), 2), NULL) as frequency,
    JSONExtractString(payload, 'after', 'status') as status,
    fromUnixTimestamp64Nano(JSONExtractInt(payload, 'after', 'created_at')) as created_at,
    fromUnixTimestamp64Nano(JSONExtractInt(payload, 'after', 'updated_at')) as updated_at,
    JSONExtractString(payload, 'op') as _cdc_operation,
    now64() as _cdc_timestamp,
    'mssql_cdc' as _cdc_source
FROM cdc_smart_meter_data_kafka
WHERE JSONExtractString(payload, 'op') IN ('c', 'u', 'r');

-- Materialized view for Kafka producer data transformation
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_smart_meter_data_mv
TO kafka_smart_meter_data_raw
AS
SELECT
    JSONExtractString(payload, 'meter_id') as meter_id,
    parseDateTime64BestEffort(JSONExtractString(payload, 'timestamp')) as timestamp,
    toDecimal64(JSONExtractFloat(payload, 'kwh_usage'), 2) as kwh_usage,
    JSONExtractInt(payload, 'voltage') as voltage,
    JSONExtractString(payload, 'customer_id') as customer_id,
    JSONExtractString(payload, 'region') as region,
    now64() as _ingestion_timestamp,
    'kafka_producer' as _source
FROM kafka_smart_meter_data_kafka;

-- =============================================
-- UNIFIED VIEWS FOR ANALYTICS
-- =============================================

-- Drop view if exists (in case of previous failures)
DROP VIEW IF EXISTS unified_smart_meter_data;

-- Unified view combining both data sources
CREATE VIEW unified_smart_meter_data AS
SELECT 
    meter_id,
    timestamp,
    CAST(NULL as Nullable(String)) as region,
    CAST(NULL as Nullable(String)) as customer_id,
    energy_consumption as consumption_kwh,
    voltage,
    current_reading as current_amps,
    power_factor,
    frequency,
    status,
    id as cdc_id,
    created_at,
    updated_at,
    'mssql_cdc' as data_source,
    _cdc_operation as operation_type
FROM cdc_smart_meter_data_raw
WHERE _cdc_operation IN ('c', 'u', 'r')

UNION ALL

SELECT 
    meter_id,
    timestamp,
    region,
    customer_id,
    kwh_usage as consumption_kwh,
    voltage,
    NULL as current_amps,
    NULL as power_factor,
    NULL as frequency,
    'active' as status,
    NULL as cdc_id,
    timestamp as created_at,
    timestamp as updated_at,
    'kafka_producer' as data_source,
    'c' as operation_type
FROM kafka_smart_meter_data_raw;

-- =============================================
-- ANALYTICAL VIEWS
-- =============================================

-- Daily energy consumption summary
CREATE VIEW IF NOT EXISTS daily_energy_consumption AS
SELECT
    toDate(timestamp) as consumption_date,
    region,
    data_source,
    countDistinct(meter_id) as unique_meters,
    countDistinct(customer_id) as unique_customers,
    sum(consumption_kwh) as total_energy_kwh,
    avg(voltage) as avg_voltage,
    avgIf(current_amps, current_amps IS NOT NULL) as avg_current_amps,
    max(consumption_kwh) as max_consumption_kwh,
    count() as total_readings
FROM unified_smart_meter_data
GROUP BY consumption_date, region, data_source;

-- System overview
CREATE VIEW IF NOT EXISTS system_overview AS
SELECT
    data_source,
    count() as total_records,
    min(timestamp) as earliest_record,
    max(timestamp) as latest_record,
    countDistinct(meter_id) as unique_meters,
    countDistinct(region) as unique_regions
FROM unified_smart_meter_data
GROUP BY data_source;

-- =============================================
-- VERIFICATION QUERIES
-- =============================================

-- Verify all tables and views were created
SELECT 
    name,
    engine,
    if(engine = 'View', 'VIEW', 'TABLE') as type
FROM system.tables 
WHERE database = 'telecom_analytics'
ORDER BY type, name;

-- Test data insertion (optional test data)
INSERT INTO cdc_smart_meter_data_raw 
(id, meter_id, timestamp, energy_consumption, voltage, current_reading, status)
VALUES 
(1, 'test_meter_cdc_001', now(), 15.5, 220.0, 10.2, 'active'),
(2, 'test_meter_cdc_002', now(), 18.3, 219.5, 11.1, 'active');

INSERT INTO kafka_smart_meter_data_raw 
(meter_id, timestamp, kwh_usage, voltage, customer_id, region)
VALUES 
('test_meter_kafka_001', now(), 12.7, 220, 'cust_001', 'north'),
('test_meter_kafka_002', now(), 14.2, 221, 'cust_002', 'south');

-- Test unified view
SELECT 
    'Unified View Test' as test,
    count(*) as total_records,
    countDistinct(data_source) as data_sources
FROM unified_smart_meter_data;

-- Test analytical views
SELECT 'Daily Consumption View' as test, count(*) as records FROM daily_energy_consumption;
SELECT 'System Overview View' as test, count(*) as records FROM system_overview;

SELECT 'Schema creation completed successfully!' as status;
