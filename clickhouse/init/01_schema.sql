-- GPU telemetry in ClickHouse: ingest from Kafka, detect at insert time via MVs.
CREATE DATABASE IF NOT EXISTS telemetry;

-- 1. Kafka consumer table (reads telemetry.gpu.raw as JSON)
CREATE TABLE IF NOT EXISTS telemetry.kafka_gpu_raw
(
    host_id String, rack String, gpu_index UInt8, gpu_uuid String,
    event_time String,
    gpu_util Float32, sm_active Float32, mem_used_gb Float32,
    temp_c Float32, power_w Float32, energy_j_total Float64,
    ecc_dbe UInt8, xid_error UInt8
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:19092',
         kafka_topic_list = 'telemetry.gpu.raw',
         kafka_group_name = 'clickhouse-gpu',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 2;

-- 2. Main storage: MergeTree ordered for per-host time scans
CREATE TABLE IF NOT EXISTS telemetry.gpu_metrics
(
    event_time DateTime64(3),
    host_id LowCardinality(String), rack LowCardinality(String),
    gpu_index UInt8, gpu_uuid String,
    gpu_util Float32, sm_active Float32, mem_used_gb Float32,
    temp_c Float32, power_w Float32, energy_j_total Float64,
    ecc_dbe UInt8, xid_error UInt8
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (host_id, gpu_index, event_time)
TTL toDateTime(event_time) + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_ingest TO telemetry.gpu_metrics AS
SELECT parseDateTime64BestEffort(event_time) AS event_time,
       host_id, rack, gpu_index, gpu_uuid,
       gpu_util, sm_active, mem_used_gb, temp_c, power_w, energy_j_total,
       ecc_dbe, xid_error
FROM telemetry.kafka_gpu_raw;

-- 3. Alerts: detection happens AT INSERT TIME, inside the database
CREATE TABLE IF NOT EXISTS telemetry.alerts
(
    detected_at DateTime DEFAULT now(),
    rule LowCardinality(String),
    severity LowCardinality(String),
    host_id LowCardinality(String),
    gpu_index UInt8,
    detail String,
    event_time DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (detected_at, rule, host_id)
TTL detected_at + INTERVAL 7 DAY;

-- Cryptojacking signature: pinned util, low real SM activity
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_detect_cryptojack TO telemetry.alerts AS
SELECT now() AS detected_at, 'cryptojack_suspect' AS rule, 'high' AS severity,
       host_id, gpu_index,
       concat('util=', toString(gpu_util), ' sm_active=', toString(sm_active),
              ' power_w=', toString(power_w)) AS detail,
       parseDateTime64BestEffort(event_time) AS event_time
FROM telemetry.kafka_gpu_raw
WHERE gpu_util > 95 AND sm_active < 30;

-- Hardware fault: ECC double-bit / XID -> drain the node
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_detect_hw_fault TO telemetry.alerts AS
SELECT now() AS detected_at, 'hw_fault' AS rule, 'critical' AS severity,
       host_id, gpu_index,
       concat('ecc_dbe=', toString(ecc_dbe), ' xid=', toString(xid_error)) AS detail,
       parseDateTime64BestEffort(event_time) AS event_time
FROM telemetry.kafka_gpu_raw
WHERE ecc_dbe > 0 OR xid_error > 0;

-- Thermal
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_detect_thermal TO telemetry.alerts AS
SELECT now() AS detected_at, 'thermal' AS rule, 'medium' AS severity,
       host_id, gpu_index,
       concat('temp_c=', toString(temp_c)) AS detail,
       parseDateTime64BestEffort(event_time) AS event_time
FROM telemetry.kafka_gpu_raw
WHERE temp_c > 92;
