-- Synthetic history for the "data scale" judging criterion: 50M rows in ~1 min.
-- Run:  docker exec -it clickhouse clickhouse-client --queries-file /backfill/backfill.sql
INSERT INTO telemetry.gpu_metrics
SELECT
    now64(3) - toIntervalSecond(intDiv(number, 16000) * 5)           AS event_time,
    concat('host-', leftPad(toString(number % 2000), 4, '0'))        AS host_id,
    concat('rack-', leftPad(toString(number % 40), 2, '0'))          AS rack,
    toUInt8(intDiv(number, 2000) % 8)                                AS gpu_index,
    concat('GPU-host-', toString(number % 2000), '-', toString(intDiv(number, 2000) % 8)) AS gpu_uuid,
    toFloat32(30 + 50 * abs(sin(number / 97.0)) + (cityHash64(number) % 20)) AS gpu_util,
    toFloat32(20 + 40 * abs(sin(number / 101.0)))                    AS sm_active,
    toFloat32(10 + (cityHash64(number + 7) % 60))                    AS mem_used_gb,
    toFloat32(35 + 35 * abs(sin(number / 89.0)))                     AS temp_c,
    toFloat32(100 + 280 * abs(sin(number / 97.0)))                   AS power_w,
    toFloat64(number)                                                AS energy_j_total,
    toUInt8(cityHash64(number) % 100000 = 0)                         AS ecc_dbe,
    toUInt8(0)                                                       AS xid_error
FROM numbers(50000000);

-- Latency showcase queries (run after backfill, note the timings):
-- SELECT count() FROM telemetry.gpu_metrics;
-- SELECT host_id, max(temp_c) FROM telemetry.gpu_metrics GROUP BY host_id ORDER BY 2 DESC LIMIT 10;
-- SELECT toStartOfMinute(event_time) m, avg(gpu_util) FROM telemetry.gpu_metrics GROUP BY m ORDER BY m DESC LIMIT 60;
