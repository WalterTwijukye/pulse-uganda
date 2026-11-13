{{ config(materialized='table') }}

WITH facts AS (
    SELECT * FROM {{ ref('fct_cdr_events') }}
)

SELECT
    call_date,
    district_name,
    region_name,
    operator_name,
    radio_generation,
    COUNT(*) AS total_calls,
    COUNT(*) FILTER (WHERE NOT is_dropped_call) AS completed_calls,
    COUNT(*) FILTER (WHERE is_dropped_call) AS dropped_calls,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_dropped_call) / NULLIF(COUNT(*), 0), 2) AS drop_call_rate_pct,
    CASE 
        WHEN ROUND(100.0 * COUNT(*) FILTER (WHERE is_dropped_call) / NULLIF(COUNT(*), 0), 2) < 2.0 THEN true 
        ELSE false 
    END AS meets_ucc_qos_standard,
    ROUND(AVG(duration_sec), 1) AS avg_call_duration_sec,
    ROUND(AVG(rsrp_dbm), 2) AS avg_rsrp_dbm,
    ROUND(AVG(rsrq_db), 2) AS avg_rsrq_db,
    ROUND(AVG(latency_ms), 1) AS avg_latency_ms,
    COUNT(*) FILTER (WHERE signal_quality = 'POOR') AS poor_signal_calls,
    COUNT(*) FILTER (WHERE is_rain_fade_risk) AS rain_affected_calls,
    COUNT(*) FILTER (WHERE is_rain_fade_risk AND is_dropped_call) AS rain_dropped_calls,
    COUNT(*) FILTER (WHERE NOT is_rain_fade_risk AND is_dropped_call) AS dry_dropped_calls
FROM facts
GROUP BY
    call_date,
    district_name,
    region_name,
    operator_name,
    radio_generation
ORDER BY
    call_date,
    district_name,
    operator_name
