-- Check the CDC capture table for changes
SELECT * FROM cdc.dbo_smart_meter_data_CT 
ORDER BY __$start_lsn DESC;
GO

-- More detailed CDC change inspection
SELECT 
    __$operation,
    CASE __$operation
        WHEN 1 THEN 'DELETE'
        WHEN 2 THEN 'INSERT' 
        WHEN 3 THEN 'UPDATE (before)'
        WHEN 4 THEN 'UPDATE (after)'
    END AS operation_type,
    id,
    meter_id,
    energy_consumption,
    __$start_lsn,
    __$seqval
FROM cdc.dbo_smart_meter_data_CT 
ORDER BY __$start_lsn DESC;
GO
