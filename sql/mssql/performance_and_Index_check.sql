-- Check if tables have proper indexes (optional)
SELECT 
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc AS index_type
FROM sys.tables t
LEFT JOIN sys.indexes i ON t.object_id = i.object_id
WHERE t.name IN ('smart_meter_data', 'daily_energy_consumption')
    AND i.index_id > 0  -- Exclude heaps
ORDER BY t.name, i.index_id;
