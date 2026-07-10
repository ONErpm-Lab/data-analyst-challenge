{{ config(materialized='view') }}

SELECT

    store,
    date,

    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,

    quantity,
    is_stream,
    is_download,
    revenue,
    currency,
    country_code,
    genre_id,
    genre_name

FROM {{ source('raw', 'sales') }}