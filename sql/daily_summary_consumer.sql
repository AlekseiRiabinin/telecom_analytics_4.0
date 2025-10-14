CREATE MATERIALIZED VIEW IF NOT EXISTS daily_summary_consumer 
TO smart_meter_daily_summary AS
SELECT 
    toDate(event_time) AS date,
    region,
    uniq(customer_id) AS total_customers,
    sum(kwh_usage) AS total_kwh_usage,
    total_kwh_usage / total_customers AS avg_kwh_per_customer,
    max(voltage) AS max_voltage,
    min(voltage) AS min_voltage,
    argMax(hour_of_day, kwh_usage) AS peak_hour
FROM smart_meter_processed
GROUP BY date, region;
