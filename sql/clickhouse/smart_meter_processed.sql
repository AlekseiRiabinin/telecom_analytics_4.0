CREATE TABLE IF NOT EXISTS smart_meter_processed (
    event_time DateTime64(3),
    meter_id String,
    customer_id String,
    region String,
    kwh_usage Float64,
    voltage Float64,
    -- Derived fields
    hour_of_day UInt8,
    day_of_week String,
    usage_category String,
    -- Metadata
    processed_at DateTime64(3) DEFAULT now(),
    raw_timestamp String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (region, meter_id, event_time)
SETTINGS index_granularity = 8192;
