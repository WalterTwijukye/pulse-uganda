# 🛰️ 24-Hour Production Observation & Maintenance Log
## PulseUganda Mobile Network Quality Intelligence Platform

This document serves as the live operational telemetry and incident observation log for the 24-hour observation window of the PulseUganda data platform. It records system stability, streaming consumer lag, quarantine anomaly rates, Delta Lake storage progression, and orchestrator daemon health under continuous operation.

---

## 1. System Pre-Flight Checklist

Before initiating the observation window, verify all core services and environmental controls:

- [ ] **WSL2 / Host Memory Limit:** Capped to 4GB (`.wslconfig`) to protect commodity hardware.
- [ ] **Docker Containers Active & Healthy:**
  - [ ] `telecom-postgres` (PostgreSQL 16 on `localhost:5434`, health: `healthy`)
  - [ ] `telecom-rustfs` (RustFS S3 on `localhost:9000`, console: `localhost:9001`)
  - [ ] `telecom-redpanda` (Redpanda Kafka broker on `localhost:9092`)
- [ ] **S3 Bucket Topology Verified:**
  - [ ] `s3://de-bronze/`
  - [ ] `s3://de-silver/`
  - [ ] `s3://de-gold/`
  - [ ] `s3://de-quarantine/`
- [ ] **Orchestration & Sensors Active:**
  - [ ] Dagster daemon running (`dagster-daemon run` or `dagster dev`)
  - [ ] `s3_cdr_sensor` enabled with cursor state initialized
  - [ ] Daily schedule `daily_telecom_schedule` armed

---

## 2. Telemetry Metrics Tracked

| Metric Name | Measurement Methodology | Target / Normal Threshold | SLA Breach Threshold |
| :--- | :--- | :--- | :--- |
| **Watermark Arrival Lag** | Time difference between CDR `event_time` and Bronze S3 object `LastModified`. | $< 60$ seconds | $> 300$ seconds |
| **Quarantine Rate** | Ratio of invalid rows diverted to `s3://de-quarantine/` vs total Bronze ingested. | $< 1.5\%$ (known border towers) | $> 5.0\%$ (data contract breach) |
| **Micro-Batch MERGE Latency** | Delta-RS intra-batch compaction + `WHEN MATCHED` MERGE commit duration. | $< 5.0$ seconds / 1k rows | $> 15.0$ seconds |
| **Delta Transaction Log Growth** | Count and size of JSON commits in `s3://de-silver/*/_delta_log/`. | Compact JSON commits ($\sim 1\text{ KB/v}$) | Uncompacted small files |
| **Orchestrator Daemon Memory** | Process memory consumption of `dagster-daemon` / `dagster dev`. | $< 350$ MB RAM | $> 600$ MB (memory leak) |

---

## 3. 24-Hour Live Observation Log

| Timestamp (UTC) | Event / Tick | Rows Processed | Quarantine Count | Latency (sec) | Drift / Issues Observed | Action Taken | Engineer |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| *2026-11-13 10:00* | *System Initialization* | 0 | 0 | 0.0s | Pre-flight baseline established; all 3 Docker containers UP. | Baseline recorded | DE-OnCall |
| *2026-11-13 12:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-13 14:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-13 16:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-13 18:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-13 20:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-13 22:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-14 00:00* | *Nightly Batch Trigger* | - | - | - | Pending scheduled run... | - | - |
| *2026-11-14 02:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-14 04:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-14 06:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-14 08:00* | *Hourly Observation* | - | - | - | Pending live tick... | - | - |
| *2026-11-14 10:00* | *Window Close & Audit* | - | - | - | Final audit & log consolidation. | - | - |

---

## 4. Post-Observation Audit & Exit Sign-Off

Upon conclusion of the 24-hour observation cycle, complete the following verifications:

1. **Idempotency Verification:** Execute `python pipeline/silver_weather.py --dt 2026-10-12` and confirm 0 row count delta.
2. **Delta Lake VACUUM & Retention:** Audit commit history via `dt.history()`; verify checkpoint snapshots are uncorrupted.
3. **Quarantine Audit:** Inspect `s3://de-quarantine/` partitions to ensure no unexpected production schemas leaked into the DLQ.
4. **dbt Data Integrity:** Run `dbt test --profiles-dir gold/` and confirm 18/18 tests pass with 0 warnings or errors.

*Signed Off By:* `Lead Data Engineer / SRE`  
*Date:* `2026-11-14`
