{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_towers') }}
),

unknown_tower AS (
    SELECT
        -1 AS tower_id,
        'UNKNOWN' AS radio,
        'OTHER' AS radio_generation,
        0 AS mcc,
        0 AS net,
        'Unknown Operator' AS operator_name,
        0 AS area,
        0 AS cell,
        'Unknown / Border Area' AS district_name,
        'UG0000' AS district_pcode,
        'Unknown Region' AS region_name,
        32.2903 AS lon,
        1.3733 AS lat,
        0 AS range_meters,
        0 AS samples,
        cast('2026-01-01 00:00:00' AS timestamp) AS created_ts,
        cast('2026-01-01 00:00:00' AS timestamp) AS updated_ts
)

SELECT
    tower_id,
    radio,
    CASE 
        WHEN radio = 'GSM' THEN '2G'
        WHEN radio = 'UMTS' THEN '3G'
        WHEN radio = 'LTE' THEN '4G'
        WHEN radio = 'NR' THEN '5G'
        ELSE 'OTHER'
    END AS radio_generation,
    mcc,
    net,
    CASE 
        WHEN mcc = 641 AND net = 1 THEN 'Airtel Uganda'
        WHEN mcc = 641 AND net = 10 THEN 'MTN Uganda'
        WHEN mcc = 641 AND net = 11 THEN 'Uganda Telecom (UTL)'
        WHEN mcc = 641 AND net = 14 THEN 'Africell / Orange'
        ELSE 'Other Operator'
    END AS operator_name,
    area,
    cell,
    district_name,
    district_pcode,
    region_name,
    lon,
    lat,
    range_meters,
    samples,
    created_ts,
    updated_ts
FROM base

UNION ALL

SELECT * FROM unknown_tower
