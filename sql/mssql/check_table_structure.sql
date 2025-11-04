-- Check if tables exist and get row counts
SELECT 
    t.name AS table_name,
    p.rows AS row_count
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE t.name IN ('smart_meter_data', 'daily_energy_consumption')
    AND p.index_id IN (0, 1) -- 0=Heap, 1=Clustered Index
GROUP BY t.name, p.rows;
