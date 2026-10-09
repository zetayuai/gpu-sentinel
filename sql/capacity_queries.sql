-- Run in Trino:  docker exec -it trino trino
-- (catalog: iceberg, schema: telemetry)

-- 1. Fleet utilization trend, 5-min buckets  (dashboards / trends)
SELECT date_trunc('minute', window_start) AS ts,
       round(avg(avg_util), 1)  AS fleet_avg_util,
       round(approx_percentile(max_util, 0.95), 1) AS p95_util
FROM iceberg.telemetry.gpu_metrics_1m
GROUP BY 1 ORDER BY 1 DESC LIMIT 30;

-- 2. Headroom: P95 vs capacity at 75% target  (capacity planning)
WITH p AS (
  SELECT approx_percentile(avg_util, 0.95) AS p95
  FROM iceberg.telemetry.gpu_metrics_1m
  WHERE window_start > current_timestamp - INTERVAL '1' HOUR
)
SELECT round(p95, 1) AS p95_util,
       round(75 - p95, 1) AS headroom_pct,
       CASE WHEN p95 > 75 THEN 'ORDER HARDWARE' ELSE 'OK' END AS verdict
FROM p;

-- 3. Hot GPUs: thermal risk  (alert candidates)
SELECT host_id, gpu_index, max(max_temp) AS peak_temp
FROM iceberg.telemetry.gpu_metrics_1m
GROUP BY 1, 2 HAVING max(max_temp) > 80
ORDER BY 3 DESC LIMIT 10;

-- 4. Wasted capacity: allocated but idle  (<20% SM active)
SELECT host_id, gpu_index,
       round(avg(avg_sm_active), 1) AS avg_sm_active,
       round(avg(avg_power), 0) AS avg_power_w
FROM iceberg.telemetry.gpu_metrics_1m
GROUP BY 1, 2 HAVING avg(avg_sm_active) < 20
ORDER BY 3 LIMIT 10;

-- 5. Hardware faults  (ECC / XID -> drain the node)
SELECT host_id, gpu_index, sum(ecc_dbe) AS ecc_dbe, sum(xid_errors) AS xid
FROM iceberg.telemetry.gpu_metrics_1m
GROUP BY 1, 2 HAVING sum(ecc_dbe) + sum(xid_errors) > 0;

-- 6. Iceberg features to demo: time travel + snapshots
SELECT * FROM iceberg.telemetry."gpu_metrics_1m$snapshots" ORDER BY committed_at DESC LIMIT 5;
-- SELECT count(*) FROM iceberg.telemetry.gpu_metrics_1m FOR TIMESTAMP AS OF TIMESTAMP '2026-08-29 20:00:00 UTC';

-- 7. Schema evolution to demo (safe, ID-based):
-- ALTER TABLE iceberg.telemetry.gpu_metrics_1m ADD COLUMN avg_mem_used_gb DOUBLE;
