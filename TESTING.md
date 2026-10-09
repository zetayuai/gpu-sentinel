# Run & Test — gpu-sentinel

## 0. Prereqs
- Docker Desktop running, 8-10 GB RAM allocated
- Python 3 on host: `pip3 install kafka-python-ng`
- `.env` next to docker-compose.yml (copy from .env.example), at minimum OPENAI_API_KEY

## 1. Start the stack (~3-5 min first time)
```bash
docker compose up -d --build
docker compose ps        # expect: kafka, clickhouse, producer, copilot, minio, rest,
                         # spark, trino, prometheus, grafana, kafka-exporter all Up
```

## 2. Test each stage (in order — each proves the previous)

### T1. Producer -> Kafka
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic telemetry.gpu.raw --max-messages 3
```
PASS: 3 JSON rows with host_id/gpu_util/sm_active. Also http://localhost:8000/metrics
shows telemetry_samples_sent_total increasing.

### T2. Kafka -> ClickHouse ingest
```bash
docker exec clickhouse clickhouse-client -q "SELECT count() FROM telemetry.gpu_metrics"
sleep 10
docker exec clickhouse clickhouse-client -q "SELECT count() FROM telemetry.gpu_metrics"
```
PASS: count grows (~16k rows / 5s). FAIL at 0: `docker logs clickhouse 2>&1 | grep -i kafka`
(usually ClickHouse started before the topic existed -> `docker restart clickhouse`).

### T3. Insert-time detection (the core demo)
```bash
python3 attacker/inject_incident.py &          # 10-min incident
sleep 15
docker exec clickhouse clickhouse-client -q \
 "SELECT detected_at, rule, severity, host_id, gpu_index, detail
  FROM telemetry.alerts ORDER BY detected_at DESC LIMIT 10"
```
PASS: cryptojack_suspect rows for host-0007 and hw_fault for host-0013 within ~15s of
injection. That delta IS your event->alert latency number.

### T4. Scale + query latency
```bash
docker exec clickhouse clickhouse-client --queries-file /backfill/backfill.sql   # 50M rows
docker exec clickhouse clickhouse-client --time -q \
 "SELECT host_id, max(temp_c) FROM telemetry.gpu_metrics GROUP BY host_id ORDER BY 2 DESC LIMIT 10"
```
PASS: aggregation over 50M+ rows returns in well under 1s; note the printed time for judges.

### T5. IR Copilot (http://localhost:8501)
1. Pick "Show the latest 20 alerts..." -> Draft investigation
2. Review the generated SQL -> Approve & run
3. PASS: results + ms latency caption + verdict citing PB-001/PB-002 steps,
   sidebar shows per-step tokens/cost.
Guardrail test: type "delete all alerts" -> must be refused (SELECT-only).

### T6. Ops path
- http://localhost:9090/targets  -> producer, kafka, prometheus all UP
- http://localhost:3000 (admin/admin) -> Explore -> Prometheus:
  rate(telemetry_samples_sent_total[1m]); ClickHouse datasource -> telemetry.alerts

### T7. Lakehouse path (optional, for the historical story)
```bash
docker exec -it spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /home/iceberg/jobs/streaming_job.py          # leave running; UI :4040
# after ~2 min, in another terminal:
docker exec -it trino trino --execute "SELECT count(*) FROM iceberg.telemetry.gpu_metrics_1m"
```

## 3. Reset between demo runs
```bash
docker exec clickhouse clickhouse-client -q "TRUNCATE TABLE telemetry.alerts"
docker exec clickhouse clickhouse-client -q "TRUNCATE TABLE telemetry.gpu_metrics"   # optional
```
Full reset: `docker compose down -v && docker compose up -d --build`

## Common failures
| Symptom | Fix |
|---|---|
| gpu_metrics stays 0 | restart clickhouse (Kafka table needs topic to exist) |
| copilot error "OPENAI_API_KEY" | set it in .env, `docker compose up -d copilot` |
| copilot SQL errors | wrong backend dialect — check COPILOT_BACKEND env |
| port already in use | another local service on 8080/9090/3000 — stop it or edit compose ports |
| Mac fans / OOM | lower NUM_HOSTS in compose to 500, `docker compose up -d producer` |
