SELECT 
    name AS table_name,
    engine AS table_engine
FROM system.tables 
WHERE database = 'telecom_data'
ORDER BY engine, name;
