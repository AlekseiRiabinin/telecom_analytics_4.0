-- Check recent data in smart_meter_data table
SELECT TOP 20 
    meter_id,
    timestamp,
    energy_consumption,
    voltage,
    current_reading,
    power_factor,
    frequency
FROM smart_meter_data 
ORDER BY timestamp DESC;

-- Check data quality metrics for smart_meter_data
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT meter_id) AS unique_meters,
    MIN(timestamp) AS earliest_timestamp,
    MAX(timestamp) AS latest_timestamp,
    AVG(energy_consumption) AS avg_energy_consumption,
    AVG(voltage) AS avg_voltage,
    AVG(current_reading) AS avg_current,
    SUM(CASE WHEN energy_consumption IS NULL THEN 1 ELSE 0 END) AS null_energy_count,
    SUM(CASE WHEN voltage IS NULL THEN 1 ELSE 0 END) AS null_voltage_count
FROM smart_meter_data;
