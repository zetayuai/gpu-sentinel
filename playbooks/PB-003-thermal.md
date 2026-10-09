# PB-003: Thermal Event
Severity: MEDIUM (>85C sustained 5 min), HIGH (>92C any sample or throttle flags set).
Response steps:
1. Check rack-level correlation: multiple hosts in one rack = cooling failure, page Facilities.
2. Single GPU hot: reduce power cap (nvidia-smi -pl) rather than killing the job.
3. HIGH: migrate workload off host, then investigate airflow/paste before returning to pool.
Owner: Infra on-call. Never ignore a >92C reading.
