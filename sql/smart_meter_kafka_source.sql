CREATE TABLE IF NOT EXISTS smart_meter_kafka_source (
    meter_id String,
    timestamp String,
    kwh_usage Float64,
    voltage Float64,
    customer_id String,
    region String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9095',
    kafka_topic_list = 'smart_meter_data',
    kafka_group_name = 'clickhouse_telecom_consumer',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10,
    kafka_max_block_size = 1048576;
