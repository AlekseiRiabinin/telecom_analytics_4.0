-- Validate data ranges (based on the transformation rules)
SELECT 
    'smart_meter_data' AS table_name,
    COUNT(*) AS total_records,
    SUM(CASE WHEN energy_consumption BETWEEN 0.5 AND 5.0 THEN 1 ELSE 0 END) AS valid_energy_range,
    SUM(CASE WHEN voltage BETWEEN 230 AND 240 THEN 1 ELSE 0 END) AS valid_voltage_range,
    SUM(CASE WHEN current_reading BETWEEN 1.0 AND 15.0 THEN 1 ELSE 0 END) AS valid_current_range,
    SUM(CASE WHEN power_factor BETWEEN 0.85 AND 0.99 THEN 1 ELSE 0 END) AS valid_power_factor,
    SUM(CASE WHEN frequency BETWEEN 49.8 AND 50.2 THEN 1 ELSE 0 END) AS valid_frequency
FROM smart_meter_data;

-- Check for any NULL values that shouldn't exist after transformation
SELECT 
    COUNT(*) AS records_with_nulls,
    SUM(CASE WHEN meter_id IS NULL THEN 1 ELSE 0 END) AS null_meter_ids,
    SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamps,
    SUM(CASE WHEN energy_consumption IS NULL THEN 1 ELSE 0 END) AS null_energy,
    SUM(CASE WHEN voltage IS NULL THEN 1 ELSE 0 END) AS null_voltage
FROM smart_meter_data;
