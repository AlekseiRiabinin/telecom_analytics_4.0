-- Check daily aggregates
SELECT TOP 10 
    meter_id,
    date,
    total_energy,
    avg_voltage,
    avg_current,
    max_consumption,
    record_count,
    created_at
FROM daily_energy_consumption 
ORDER BY date DESC, meter_id;

-- Summary of daily aggregates
SELECT 
    COUNT(*) AS total_daily_records,
    COUNT(DISTINCT meter_id) AS meters_with_daily_data,
    MIN(date) AS earliest_date,
    MAX(date) AS latest_date,
    AVG(total_energy) AS avg_daily_energy,
    SUM(total_energy) AS total_energy_all_meters
FROM daily_energy_consumption;
