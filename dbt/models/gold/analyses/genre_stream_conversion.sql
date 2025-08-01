{{ 
  config(
    materialized='table'
    ) 
}}

WITH genre_data AS (
  SELECT
    genre_name,
    SUM(CASE WHEN is_stream = 1 THEN quantity ELSE 0 END) AS total_streams,
    SUM(CASE WHEN is_stream = 1 THEN revenue ELSE 0 END) AS stream_revenue
  FROM {{ ref('int_music_sales') }}
  GROUP BY genre_name
)
SELECT
  genre_name,
  total_streams,
  stream_revenue,
  stream_revenue / NULLIF(total_streams, 0) AS revenue_per_stream
FROM genre_data
ORDER BY revenue_per_stream DESC
