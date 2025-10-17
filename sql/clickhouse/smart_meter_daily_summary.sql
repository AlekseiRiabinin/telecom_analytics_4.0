CREATE TABLE IF NOT EXISTS smart_meter_daily_summary (
    date Date,
    region String,
    total_customers UInt32,
    total_kwh_usage Float64,
    avg_kwh_per_customer Float64,
    max_voltage Float64,
    min_voltage Float64,
    peak_hour UInt8,
    last_updated DateTime64(3) DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, region)
SETTINGS index_granularity = 8192;
