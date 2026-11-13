{{ config(materialized='view') }}

SELECT
    call_id,
    caller_msisdn,
    callee_msisdn,
    tower_id,
    event_type,
    call_ts,
    call_date,
    date_trunc('hour', call_ts) AS call_hour,
    duration_sec,
    termination_code,
    is_dropped_call,
    rsrp_dbm,
    rsrq_db,
    signal_quality,
    latency_ms,
    op,
    updated_at_ts
FROM delta_scan('s3://de-silver/cdrs')
