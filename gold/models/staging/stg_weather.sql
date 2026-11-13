{{ config(materialized='view') }}

SELECT
    district_name,
    district_pcode,
    latitude,
    longitude,
    observed_at_ts,
    observation_date,
    date_trunc('hour', observed_at_ts) AS observed_hour,
    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    weather_code,
    weather_condition,
    rain_severity,
    ingested_at
FROM delta_scan('s3://de-silver/weather')
