{{ 
  config(
    materialized='table'
    ) 
}}


WITH monthly AS (
  SELECT
    country_code,
    EXTRACT(MONTH FROM DATE(sale_date)) AS month,
    SUM(revenue) AS revenue
  FROM {{ ref('int_music_sales') }}
  GROUP BY 1, 2
),
with_lag AS (
  SELECT
    country_code,
    month,
    revenue,
    LAG(revenue) OVER (PARTITION BY country_code ORDER BY month) AS prev_revenue
  FROM monthly
)
SELECT
  country_code,
  month,
  revenue,
  prev_revenue,
  (revenue - prev_revenue) / NULLIF(prev_revenue, 0) AS monthly_growth
FROM with_lag
