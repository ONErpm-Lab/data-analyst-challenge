{{ 
  config(
    materialized='table'
    ) 
}}

SELECT
  is_stream,
  is_download,
  store,
  genre_name,
  SUM(revenue) AS total_revenue,
  COUNT(DISTINCT product) AS num_products
FROM {{ ref('int_music_sales') }}
GROUP BY 1, 2, 3, 4
ORDER BY total_revenue DESC
LIMIT 20
