{{ config(materialized='view') }} 

SELECT 
year, 
month, 
store, 
country_code, 
genre_id, 
genre_name, 
COUNT(*) AS sales, 
SUM(quantity) AS quantity, 
SUM(revenue) AS revenue, 
SUM(streams) AS streams, 
SUM(downloads) AS downloads, 
SUM(revenue) / NULLIF(COUNT(*), 0) AS avg_ticket, 
SUM(revenue) / NULLIF(SUM(streams), 0) AS avg_revenue_per_stream, 
SUM(revenue) / NULLIF(SUM(downloads), 0) AS avg_revenue_per_download, 
SUM(revenue) / NULLIF(SUM(quantity), 0) AS avg_revenue_per_sale 
FROM {{ ref('int_sales_metrics') }}
GROUP BY year, month, store, country_code, genre_id, genre_name