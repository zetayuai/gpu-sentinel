# GPU Telemetry Mini-Platform (interview demo project)

The interview flow, running on your laptop: **Telemetry → Kafka → Spark Structured Streaming → S3(MinIO)/Iceberg → Trino → Prometheus/Grafana → Capacity queries**

## Prerequisites

-   Docker Desktop with **8–10 GB RAM** allocated
-   ~5 GB free disk

## Run it

```bash
# 1. Start everything (first run pulls images, ~5 min)
docker compose up -d --build

# 2. Verify Kafka is receiving telemetry (50 hosts x 4 GPUs, every 5 s)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic telemetry.gpu.raw --max-messages 3

# 3. Start the Spark streaming job (leave running in a terminal)
docker exec -it spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4 \
  /home/iceberg/jobs/streaming_job.py

# 4. After ~2 minutes, query the lakehouse with Trino
docker exec -it trino trino
  trino> SHOW TABLES FROM iceberg.telemetry;
  trino> SELECT count(*) FROM iceberg.telemetry.gpu_metrics_1m;
  -- then run the queries in sql/capacity_queries.sql
```

## UIs

What

URL

Login

Spark UI (stages, shuffle, batches)

[http://localhost:4040](http://localhost:4040)

—

Trino UI (queries)

[http://localhost:8080](http://localhost:8080)

any user

MinIO console (see the Parquet files!)

[http://localhost:9001](http://localhost:9001)

admin / password

Prometheus

[http://localhost:9090](http://localhost:9090)

—

Grafana

[http://localhost:3000](http://localhost:3000)

admin / admin

Jupyter (Spark notebooks)

[http://localhost:8888](http://localhost:8888)

—

## What to explore for each interview topic

1.  **Kafka partitioning/ordering** — topic has 8 partitions, producer keys by `host_id`: `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic telemetry.gpu.raw`
2.  **Replay** — stop the Spark job, reset the consumer offsets or just note `startingOffsets=earliest` + checkpoint; delete the checkpoint prefix in MinIO and rerun → full replay.
3.  **Spark shuffle/stages** — watch [http://localhost:4040](http://localhost:4040) while the job runs; the groupBy(window) is the shuffle boundary; `spark.sql.shuffle.partitions=8` is set deliberately.
4.  **Iceberg** — in MinIO console browse `warehouse/` → data + metadata files; in Trino query `"gpu_metrics_1m$snapshots"`, try time travel, run the ALTER TABLE in `sql/capacity_queries.sql`.
5.  **Small files** — after an hour, count files per partition in MinIO; then in spark-sql: `CALL demo.system.rewrite_data_files(table => 'telemetry.gpu_metrics_1m')` — compaction, live.
6.  **Prometheus/Grafana** — Grafana → Explore → queries: `rate(telemetry_samples_sent_total[1m])` (pipeline throughput), `sim_fleet_gpu_util_avg` (fleet load), `kafka_consumergroup_lag` (THE pipeline SLI).
7.  **Capacity planning** — `sql/capacity_queries.sql`: P95 vs 75% target, headroom verdict, idle-GPU waste, thermal outliers, ECC/XID faults.
8.  **Failure handling** — `docker stop kafka` for 30 s, watch producer errors in its logs, restart, watch the stream catch up (consumer lag drains) with no data loss.

## Talking-point map (file → interview topic)

-   `producer/gpu_telemetry_producer.py` → collection agents, acks=all, idempotence, keyed ordering, self-metrics
-   `docker-compose.yml` kafka section → partitions, retention, listeners
-   `spark/streaming_job.py` → schema enforcement, watermark, windowed agg, checkpoint, trigger
-   `trino/catalog/iceberg.properties` → REST catalog, S3 path-style
-   `sql/capacity_queries.sql` → headroom/P95/saturation/waste + Iceberg time travel
-   `prometheus/prometheus.yml` → monitor the pipeline itself, not just the fleet

## Scale story ("laptop → 50,000 servers")

Same architecture; what changes: 8→256 partitions RF=3 multi-AZ, EMR-on-EKS instead of one container, real S3 + Glue catalog, RocksDB state store, Thanos-sharded Prometheus, scheduled Iceberg compaction/snapshot-expiry, schema registry, DLQ consumer, WAP publishing. See `../telemetry-architecture.md` for the full design.
## Incident Response Copilot (hackathon layer)

Natural-language incident investigation over the lakehouse: question → OpenAI generates
Trino SQL (SELECT-only guardrails) → executes → LLM analyst verdict + next action.

```bash
export OPENAI_API_KEY=sk-...        # put it in .env or export before compose
docker compose up -d --build copilot
open http://localhost:8501

# give it an incident to find (10-min cryptojack on host-0007 + HW fault on host-0013):
pip3 install kafka-python-ng && python3 attacker/inject_incident.py
```

Demo script:
1. Ask: "Find hosts with avg_util above 90 but sm_active below 30 in the last hour"
   → copilot surfaces host-0007 (fake load = mining signature)
2. Ask: "Reconstruct a minute-by-minute timeline for host-0007 over the last 30 minutes"
   → temps climbing, flat 400W power → analyst verdict recommends isolation
3. Ask: "Show hosts with any ECC or XID errors" → host-0013 gpu 2 → drain recommendation

## ClickHouse real-time detection layer (hackathon primary track)

Kafka → ClickHouse Kafka-engine table → MergeTree storage + **detection materialized
views that fire at insert time** → `telemetry.alerts`. Alert latency = ingest latency (~1-2 s).
Producer scaled to 2,000 hosts × 8 GPUs = 16,000 rows every 5 s (~3,200 rows/s sustained).

```bash
docker compose up -d clickhouse && docker compose up -d --build producer copilot

# verify ingest + insert-time detection
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM telemetry.gpu_metrics"
python3 attacker/inject_incident.py        # in another terminal
docker exec -it clickhouse clickhouse-client -q \
  "SELECT detected_at, rule, severity, host_id, detail FROM telemetry.alerts ORDER BY detected_at DESC LIMIT 10"

# data-scale demo: 50M synthetic history rows, then time an aggregation
docker exec -it clickhouse clickhouse-client --queries-file /backfill/backfill.sql
docker exec -it clickhouse clickhouse-client --time -q \
  "SELECT host_id, max(temp_c) FROM telemetry.gpu_metrics GROUP BY host_id ORDER BY 2 DESC LIMIT 10"
```

### Copilot on AkashML (open models on decentralized GPU)
```bash
export OPENAI_BASE_URL=https://api.akashml.com/v1
export OPENAI_API_KEY=<akashml key>
export OPENAI_MODEL=<model id from akashml.com>   # e.g. a Llama 3.x instruct model
docker compose up -d --build copilot              # http://localhost:8501
```
Copilot shows per-query latency in ms — screenshot that for judging.
`COPILOT_BACKEND=trino` switches it to the Iceberg lakehouse (historical story).

### Judging pitch map
- **Data scale**: 50M+ rows, 3.2k rows/s live ingest, 7-day TTL
- **Latency**: insert-time detection (~1-2 s event→alert); ms-range aggregations over 50M rows
- **Insight → action**: alerts table carries rule+severity+detail; copilot turns any question
  into SQL and an analyst verdict with a recommended remediation

## Agentic layer: Senso ground truth + approval gate (Guild pattern)

Loop: prompt → **Senso.ai** retrieves org playbooks (`playbooks/*.md` are the ground truth;
upload them to Senso and set `SENSO_API_KEY`, local fallback keeps the demo alive) →
LLM drafts SQL → **human approval gate** (review/edit/reject before execution) →
ClickHouse → verdict that cites PB-001/002/003 steps, not generic advice.
Sidebar shows per-step token/cost telemetry — the signals a Guild.ai control plane tracks;
register/run the agent on Guild at the event per docs.guild.ai.

Env: SENSO_API_KEY, SENSO_BASE_URL (optional), PRICE_PER_MTOK_IN/OUT for cost display.
