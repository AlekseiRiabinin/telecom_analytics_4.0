CREATE MATERIALIZED VIEW IF NOT EXISTS smart_meter_consumer 
TO smart_meter_processed AS
SELECT 
    parseDateTimeBestEffort(timestamp) AS event_time,
    meter_id,
    customer_id,
    region,
    kwh_usage,
    voltage,
    -- Transformations
    toHour(event_time) AS hour_of_day,
    -- Use dayOfWeek (returns 1-7 where 1=Monday) and map to names
    multiIf(
        toDayOfWeek(event_time) = 1, 'Monday',
        toDayOfWeek(event_time) = 2, 'Tuesday', 
        toDayOfWeek(event_time) = 3, 'Wednesday',
        toDayOfWeek(event_time) = 4, 'Thursday',
        toDayOfWeek(event_time) = 5, 'Friday',
        toDayOfWeek(event_time) = 6, 'Saturday',
        'Sunday'
    ) AS day_of_week,
    -- Usage categories
    multiIf(
        kwh_usage < 1.0, 'LOW',
        kwh_usage < 3.0, 'MEDIUM', 
        'HIGH'
    ) AS usage_category,
    -- Store original timestamp for debugging
    timestamp AS raw_timestamp
FROM smart_meter_kafka_source;
