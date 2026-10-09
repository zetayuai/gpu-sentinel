# Project: GPU Telemetry Threat Detection 

Real-time security observability for GPU fleets. One-liner: "your observability pipeline
is already a security sensor."

## Architecture
producer (2000 hosts x 8 GPUs, 5s, JSON) -> Kafka topic telemetry.gpu.raw (8 partitions, key=host_id)
 -> ClickHouse Kafka-engine table -> telemetry.gpu_metrics (MergeTree)
    + insert-time detection MVs (cryptojack_suspect / hw_fault / thermal) -> telemetry.alerts
 -> copilot/ Streamlit app (:8501): NL question -> LLM -> SQL (SELECT-only guard) -> analyst verdict
Secondary (historical lakehouse story): Spark Structured Streaming -> Iceberg on MinIO -> Trino.
Ops: Prometheus (:9090) + Grafana (:3000, clickhouse datasource).

## Key files
- docker-compose.yml          all services
- producer/gpu_telemetry_producer.py   simulator (DCGM-style metrics)
- clickhouse/init/01_schema.sql        tables + detection MVs  (edit detections HERE)
- clickhouse/backfill.sql              50M-row synthetic history
- copilot/app.py                       IR copilot (BACKEND=clickhouse|trino; OpenAI-compatible LLM)
- attacker/inject_incident.py          demo incident (host-0007 cryptojack, host-0013 hw fault)
- spark/streaming_job.py               Iceberg path (runs via spark-submit in `spark` container)

## Conventions / gotchas
- Detection logic lives in ClickHouse MVs, not application code.
- Cryptojack signature: gpu_util>95 AND sm_active<30 (high util, low real SM activity).
- Copilot SQL guard is SELECT-only, forced LIMIT; keep it that way.
- Kafka internal listener is kafka:19092 (containers), localhost:9092 (host).
- ClickHouse native port mapped to host 9005 (9000 is MinIO).
- LLM: any OpenAI-compatible endpoint via OPENAI_BASE_URL/OPENAI_MODEL
  (AkashML for the Akash prize; Anthropic's OpenAI-compat endpoint also works).

## Commands
docker compose up -d --build                  # whole stack
python3 attacker/inject_incident.py           # create an incident
docker exec -it clickhouse clickhouse-client  # SQL shell
docker compose logs -f <service>              # debug
