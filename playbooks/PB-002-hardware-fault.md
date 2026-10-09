# PB-002: GPU Hardware Fault (ECC double-bit / XID errors)
Severity: CRITICAL for ecc_dbe>0 or xid 48/63/64/79; MEDIUM for rising single-bit trend.
Response steps:
1. DRAIN the node immediately: cordon in scheduler, let running jobs checkpoint (max 30 min grace).
2. Do NOT reschedule workloads to the same GPU. Mark gpu_uuid in the deny-list.
3. Run dcgmi diag -r 3; attach output to the RMA ticket.
4. If >2 GPUs on one host fault within 24h, suspect PSU/board: drain the whole host.
Owner: DC Hardware team. SLA: drain within 15 minutes for CRITICAL.
