"""Simulates DCGM-style GPU telemetry for N hosts and produces to Kafka.

Interview talking points baked in:
- key = host_id            -> per-host ordering on one partition
- acks=all + idempotence   -> no dup/loss on retry
- zstd-style batching      -> throughput (gzip here; zstd needs extra lib)
- exposes its OWN /metrics -> "monitor the pipeline, not just the fleet"
"""
import json, os, random, time, threading
from datetime import datetime, timezone

from kafka import KafkaProducer
from prometheus_client import Counter, Gauge, start_http_server

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
NUM_HOSTS = int(os.environ.get("NUM_HOSTS", "50"))
GPUS = int(os.environ.get("GPUS_PER_HOST", "4"))
INTERVAL = float(os.environ.get("INTERVAL_SEC", "5"))

SENT = Counter("telemetry_samples_sent_total", "Samples produced to Kafka")
ERRS = Counter("telemetry_send_errors_total", "Producer errors")
FLEET_UTIL = Gauge("sim_fleet_gpu_util_avg", "Simulated fleet-average GPU util")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode(),
    acks="all",
    enable_idempotence=True,
    linger_ms=50,
    batch_size=262144,
    compression_type="gzip",
)

class HostSim:
    """Each host has a slowly-drifting base load + diurnal-ish wave, so
    aggregates and forecasts have real structure instead of white noise."""
    def __init__(self, i):
        self.host_id = f"host-{i:04d}"
        self.rack = f"rack-{i % 10:02d}"
        self.base = random.uniform(20, 75)
        self.phase = random.uniform(0, 6.28)
        self.energy = [0.0] * GPUS  # monotonic counter per GPU

    def sample(self, t):
        rows = []
        wave = 15 * abs(__import__("math").sin(t / 600 + self.phase))
        for g in range(GPUS):
            util = max(0, min(100, random.gauss(self.base + wave, 8)))
            power = 80 + util * 3.2 + random.gauss(0, 10)          # ~80-420 W
            self.energy[g] += power * INTERVAL                      # joules
            rows.append({
                "host_id": self.host_id, "rack": self.rack,
                "gpu_index": g, "gpu_uuid": f"GPU-{self.host_id}-{g}",
                "event_time": datetime.now(timezone.utc).isoformat(),
                "gpu_util": round(util, 1),
                "sm_active": round(util * random.uniform(0.55, 0.95), 1),
                "mem_used_gb": round(util / 100 * 78 + random.uniform(0, 2), 1),
                "temp_c": round(30 + util * 0.55 + random.gauss(0, 2), 1),
                "power_w": round(power, 1),
                "energy_j_total": round(self.energy[g], 0),
                "ecc_dbe": 1 if random.random() < 0.0001 else 0,     # rare fault
                "xid_error": 1 if random.random() < 0.00005 else 0,
            })
        return rows

hosts = [HostSim(i) for i in range(NUM_HOSTS)]
start_http_server(8000)
print(f"producing {NUM_HOSTS} hosts x {GPUS} GPUs every {INTERVAL}s -> {BOOTSTRAP}")

while True:
    t = time.time()
    utils = []
    for h in hosts:
        for row in h.sample(t):
            try:
                producer.send("telemetry.gpu.raw", key=row["host_id"], value=row)
                SENT.inc()
                utils.append(row["gpu_util"])
            except Exception:
                ERRS.inc()
    FLEET_UTIL.set(sum(utils) / len(utils))
    producer.flush()
    time.sleep(max(0, INTERVAL - (time.time() - t)))
