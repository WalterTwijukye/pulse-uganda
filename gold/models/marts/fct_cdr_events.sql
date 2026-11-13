{{ config(materialized='table') }}

WITH cdrs AS (
    SELECT * FROM {{ ref('stg_cdrs') }}
),

towers AS (
    SELECT * FROM {{ ref('dim_towers') }}
),

weather AS (
    SELECT * FROM {{ ref('dim_weather') }}
)

SELECT
    c.call_id,
    c.caller_msisdn,
    c.callee_msisdn,
    coalesce(t.tower_id, -1) AS tower_id,
    coalesce(t.operator_name, 'Unknown Operator') AS operator_name,
    coalesce(t.radio_generation, 'OTHER') AS radio_generation,
    coalesce(t.radio, 'UNKNOWN') AS radio,
    coalesce(t.district_name, 'Unknown / Border Area') AS district_name,
    coalesce(t.district_pcode, 'UG0000') AS district_pcode,
    coalesce(t.region_name, 'Unknown Region') AS region_name,
    coalesce(t.lon, 32.2903) AS tower_lon,
    coalesce(t.lat, 1.3733) AS tower_lat,
    c.event_type,
    c.call_ts,
    cast(c.call_date AS date) AS call_date,
    c.call_hour,
    c.duration_sec,
    c.termination_code,
    c.is_dropped_call,
    c.rsrp_dbm,
    c.rsrq_db,
    c.signal_quality,
    c.latency_ms,
    coalesce(w.weather_condition, 'Clear Sky') AS weather_condition,
    coalesce(w.rain_severity, 'NONE') AS rain_severity,
    coalesce(w.is_rain_fade_risk, false) AS is_rain_fade_risk,
    coalesce(w.temperature_c, 22.0) AS temperature_c,
    coalesce(w.precipitation_mm, 0.0) AS precipitation_mm
FROM cdrs c
LEFT JOIN towers t
    ON c.tower_id = t.tower_id
LEFT JOIN weather w
    ON coalesce(t.district_pcode, 'UG0000') = w.district_pcode
   AND (
       c.call_hour = w.observed_hour 
       OR extract('hour' FROM c.call_ts) = extract('hour' FROM w.observed_at_ts)
   )
