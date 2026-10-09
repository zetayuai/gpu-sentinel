"""Incident injector: makes host-0007 look compromised (cryptomining signature)
and gives host-0013 a hardware fault, so the Copilot has something to find.

Run from your Mac (needs: pip install kafka-python-ng):
  python3 attacker/inject_incident.py
Or inside docker:
  docker exec gpu-producer python -c "$(cat attacker/inject_incident.py)"

Signature injected for ~10 minutes:
- host-0007: all 4 GPUs pinned at 97-100% util but sm_active ~15% (fake load),
  flat power ~400W, temps climbing -> classic cryptojack/abuse pattern
- host-0013 gpu 2: burst of ecc_dbe + xid errors -> drain-the-node case
"""
import json, os, random, time
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
DURATION_MIN = float(os.environ.get("DURATION_MIN", "10"))

p = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                  key_serializer=lambda k: k.encode(),
                  value_serializer=lambda v: json.dumps(v).encode(),
                  acks="all")

def row(host, gpu, **kw):
    base = {"host_id": host, "rack": "rack-66", "gpu_index": gpu,
            "gpu_uuid": f"GPU-{host}-{gpu}",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "gpu_util": 50.0, "sm_active": 40.0, "mem_used_gb": 30.0,
            "temp_c": 55.0, "power_w": 250.0, "energy_j_total": 0,
            "ecc_dbe": 0, "xid_error": 0}
    base.update(kw)
    return base

print(f"injecting incident for {DURATION_MIN} min -> {BOOTSTRAP}")
end = time.time() + DURATION_MIN * 60
temp = 70.0
while time.time() < end:
    temp = min(95.0, temp + 0.3)
    for g in range(4):  # cryptojack signature on host-0007
        r = row("host-0007", g,
                gpu_util=round(random.uniform(97, 100), 1),
                sm_active=round(random.uniform(10, 18), 1),
                power_w=round(400 + random.gauss(0, 3), 1),
                temp_c=round(temp + random.gauss(0, 1), 1))
        p.send("telemetry.gpu.raw", key=r["host_id"], value=r)
    # hardware fault on host-0013 gpu 2
    f = row("host-0013", 2, ecc_dbe=random.choice([0, 0, 1]),
            xid_error=random.choice([0, 1]), temp_c=88.0)
    p.send("telemetry.gpu.raw", key=f["host_id"], value=f)
    p.flush()
    time.sleep(5)
print("incident injection finished")
