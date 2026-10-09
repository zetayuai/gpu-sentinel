# PB-001: Suspected Cryptojacking / Unauthorized GPU Workload
Severity: HIGH. Applies when: gpu_util > 95% sustained with sm_active < 30%, flat power draw.
Response steps (in order):
1. Confirm signature over a 15-minute window (not a single sample).
2. Cross-check scheduler: is a legitimate job allocated to this host? If yes, escalate to job owner first.
3. If unallocated: ISOLATE the host from the network (quarantine VLAN). Do NOT power off (preserve memory forensics).
4. Capture process list and GPU context (nvidia-smi, dcgmi diag) before any remediation.
5. Rotate credentials/keys present on the host. File incident ticket severity-HIGH within 1 hour.
Owner: Infra Security on-call. SLA: isolation within 30 minutes of confirmation.
