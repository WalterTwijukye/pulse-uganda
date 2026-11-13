{{ config(materialized='view') }}

SELECT
    tower_id,
    radio,
    mcc,
    net,
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
    updated_ts,
    ingested_at
FROM delta_scan('s3://de-silver/towers')
