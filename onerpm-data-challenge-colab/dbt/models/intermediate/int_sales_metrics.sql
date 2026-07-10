{{ config(materialized='view') }} 

SELECT 
year, 
month, 
store, 
country_code, 
genre_id, 
genre_name, 
quantity, 
revenue, 
CASE WHEN is_stream THEN quantity ELSE 0 END AS streams, 
CASE WHEN is_download THEN quantity ELSE 0 END AS downloads 
FROM {{ ref('stg_sales') }}