
{{ 
  config(
    materialized='table',
    full_refresh = false,
    ) 
}}

SELECT
    store,
    DATE("date") as "date",
    product,
    quantity,
    is_stream,
    is_download,
    revenue,
    currency,
    country_code,
    genre_id,
    genre_name
FROM
  {{ source('public', 'test_data_2023_csv000') }}
UNION ALL 
SELECT
    store,
    DATE("date") as "date",
    product,
    quantity,
    is_stream,
    is_download,
    revenue,
    currency,
    country_code,
    genre_id,
    genre_name
FROM
  {{ source('public', 'test_data_2023_csv001') }}
