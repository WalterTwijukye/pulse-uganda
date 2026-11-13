{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT cast(call_date AS date) AS date_day FROM {{ ref('stg_cdrs') }}
    UNION
    SELECT DISTINCT cast(observation_date AS date) AS date_day FROM {{ ref('stg_weather') }}
)

SELECT
    date_day,
    cast(strftime(date_day, '%Y%m%d') AS integer) AS date_key,
    extract('year' FROM date_day) AS year,
    extract('quarter' FROM date_day) AS quarter,
    extract('month' FROM date_day) AS month,
    strftime(date_day, '%B') AS month_name,
    extract('day' FROM date_day) AS day_of_month,
    strftime(date_day, '%A') AS day_name,
    extract('isodow' FROM date_day) AS day_of_week,
    CASE WHEN extract('isodow' FROM date_day) IN (6, 7) THEN true ELSE false END AS is_weekend
FROM dates
