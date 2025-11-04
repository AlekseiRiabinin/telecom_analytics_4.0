-- Compare raw counts with what should be in the database
-- This helps verify if all data was loaded
SELECT 
    'Expected based on Spark logs' AS description,
    2865 AS expected_smart_meter_records
UNION ALL
SELECT 
    'Actual in database' AS description,
    (SELECT COUNT(*) FROM smart_meter_data) AS actual_records;

-- Check if we have data for all expected meter IDs
SELECT 
    COUNT(DISTINCT meter_id) AS unique_meters_in_data,
    (SELECT COUNT(DISTINCT meter_id) FROM smart_meter_data) AS unique_meters_in_db
FROM (
    -- This would need to be adjusted based on actual source data
    SELECT 'HAMILTON_185' AS meter_id UNION ALL
    SELECT 'WELLINGTON_782' UNION ALL
    SELECT 'HAMILTON_150'
    -- Add more expected meter IDs from the logs
) expected_meters;
