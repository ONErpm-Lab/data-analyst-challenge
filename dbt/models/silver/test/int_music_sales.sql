
{{ 
  config(
    materialized='table',
    full_refresh = false,
  ) 
}}

with data_2022 as (
  SELECT
    store,
    DATE(date) AS sale_date,
    product,
    quantity,
    is_stream,
    is_download,
    revenue,
    currency,
    country_code,
    genre_id,
    genre_name,
    EXTRACT(YEAR FROM DATE(date)) AS year,
    EXTRACT(MONTH FROM DATE(date)) AS month
  FROM {{ ref('test_data_2022') }}
),

data_2023 as (
  SELECT
    store,
    DATE(date) AS sale_date,
    product,
    quantity,
    is_stream,
    is_download,
    revenue,
    currency,
    country_code,
    genre_id,
    genre_name,
    EXTRACT(YEAR FROM DATE(date)) AS year,
    EXTRACT(MONTH FROM DATE(date)) AS month
  FROM {{ ref('test_data_2023') }}
),

data_2024 as (
  SELECT
    store,
    DATE(date) AS sale_date,
    product,
    quantity,
    is_stream,
    is_download,
    revenue,
    currency,
    country_code,
    genre_id,
    genre_name,
    EXTRACT(YEAR FROM DATE(date)) AS year,
    EXTRACT(MONTH FROM DATE(date)) AS month
  FROM {{ ref('test_data_2024') }}
)


SELECT * FROM data_2022
UNION ALL 
SELECT * FROM data_2023
UNION ALL 
SELECT * FROM data_2024