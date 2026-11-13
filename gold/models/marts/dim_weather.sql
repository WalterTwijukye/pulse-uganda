{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_weather') }}
)

SELECT
    md5(concat(district_pcode, '_', cast(observed_hour AS varchar))) AS weather_key,
    district_name,
    district_pcode,
    latitude,
    longitude,
    observed_at_ts,
    observed_hour,
    observation_date,
    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    weather_code,
    weather_condition,
    rain_severity,
    CASE 
        WHEN rain_severity IN ('MODERATE', 'HEAVY') THEN true 
        ELSE false 
    END AS is_rain_fade_risk
FROM base
