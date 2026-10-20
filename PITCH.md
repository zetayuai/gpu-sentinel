# GPU-Sentinel — 3-Minute Judging Pitch

## Pre-demo setup (do BEFORE walking up)
- [ ] `docker compose ps` — all Up; copilot smoke-tested
- [ ] Alerts truncated: `docker exec clickhouse clickhouse-client -q "TRUNCATE TABLE telemetry.alerts"`
- [ ] Backfill done (50M rows) — count ready: `SELECT count() FROM telemetry.gpu_metrics`
- [ ] Browser tabs open in order: :8501 (copilot), terminal, :3000 (grafana)
- [ ] `attacker/inject_incident.py` ready in a terminal — DO NOT run yet
- [ ] demo-assets/ screenshots as backup

## The pitch (speak it)

### 0:00 — Hook (20 s)
"A server with 8 GPUs is worth thousands of dollars a month to an attacker.
Cryptomining on stolen GPU compute looks like a normal training job to every
traditional security tool — EDR watches processes, SIEM watches logs, nobody
watches the GPUs themselves. We built GPU-Sentinel on one idea:
**your observability pipeline is already a security sensor.**"

### 0:20 — Architecture (30 s, show the README diagram)
"Two thousand simulated hosts, sixteen thousand GPUs, stream DCGM telemetry
through Kafka into ClickHouse. Here's the trick: detection doesn't happen in a
separate service — it happens **inside the database, at insert time**, with
materialized views. The cryptojacking signature is physics: GPU pinned at 100%
utilization but SM activity under 30% — fake load. ECC errors mean failing
hardware. Every row is evaluated as it lands."

### 0:50 — LIVE DEMO 1: attack -> alert (45 s)
[terminal] `python3 attacker/inject_incident.py`
"I'm compromising host-0007 with a cryptominer right now, and host-0013 just
developed a memory fault."
[wait ~10 s, then:]
`SELECT detected_at, rule, host_id, detail FROM telemetry.alerts WHERE rule != 'thermal' ORDER BY detected_at DESC LIMIT 10`
"Event to alert: under two seconds. At 3,200 rows per second sustained."

### 1:35 — LIVE DEMO 2: scale (20 s)
`docker exec clickhouse clickhouse-client --time -q "SELECT host_id, max(temp_c) FROM telemetry.gpu_metrics GROUP BY host_id ORDER BY 2 DESC LIMIT 10"`
"Fifty million rows. Fifty-six milliseconds. That's the ClickHouse story:
the data volume that makes detection possible no longer makes it slow."

### 1:55 — LIVE DEMO 3: the copilot (50 s)  [tab :8501]
"An alert at 2 a.m. is only useful if someone can investigate it."
- Ask: "Which hosts triggered cryptojack_suspect alerts in the last 15 minutes?"
- "The LLM drafts SQL — but it does NOT run. A human approves first." [Approve]
- "And the verdict isn't generic AI advice — it's grounded in OUR incident
  playbooks via Senso: isolate, don't power off, rotate keys — PB-001's actual
  steps. The model is an open model running on Akash decentralized GPU compute,
  and the sidebar tracks every token and cent — the control-plane telemetry
  Guild consumes."

### 2:45 — Close (15 s)
"Three numbers: fifty million rows, fifty-six millisecond queries, two seconds
from attack to alert. GPU-Sentinel — the security sensor you already have.
Repo's public, runs with one docker compose up."

## Likely judge questions
- "Real DCGM or simulated?" — Simulated at realistic rates; the schema IS
  DCGM's field set, agents swap in with zero pipeline changes.
- "False positives?" — We tuned live: thermal first fired on normal load at
  85C; raised to 92C after checking the fleet's real distribution. Detection
  engineering, not just rules.
- "Why not Prometheus alerts?" — Prometheus can't hold per-sample history at
  this cardinality or answer ad-hoc forensic SQL; we keep it for ops health.
- "SQL injection via the LLM?" — SELECT-only guard + forced LIMIT + human gate;
  we also ran Semgrep on the generated code and submitted a finding (regex
  guard bypass surface) to their track.
- "Production path?" — Same shape: DCGM-exporter -> managed Kafka -> ClickHouse
  Cloud; Spark/Iceberg/Trino path in the repo is the long-retention story.

## Fallback if live demo breaks
demo-assets/ screenshots: alerts firing, 0.056s/50M timing, copilot verdict.
Narrate over them — the numbers are already captured.
