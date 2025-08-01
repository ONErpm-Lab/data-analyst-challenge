
{{ 
  config(
    materialized='table'
    ) 
}}


SELECT
  year,
  SUM(revenue) AS total_revenue
FROM {{ ref('int_music_sales') }}
GROUP BY year
ORDER BY year

