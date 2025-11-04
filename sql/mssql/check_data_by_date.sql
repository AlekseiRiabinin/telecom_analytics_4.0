-- Check data for a specific date (replace with the processing date)
SELECT 
    COUNT(*) AS records_count,
    MIN(timestamp) AS first_reading,
    MAX(timestamp) AS last_reading
FROM smart_meter_data 
WHERE CAST(timestamp AS DATE) = '2025-11-04';

-- Check daily aggregates for a specific date
SELECT *
FROM daily_energy_consumption 
WHERE date = '2025-11-04'
ORDER BY total_energy DESC;
