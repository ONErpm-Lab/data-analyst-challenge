{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT
        store,
        TRY_CAST(date AS DATE) AS date,
        product,
        TRY_CAST(quantity AS BIGINT) AS quantity,
        TRY_CAST(is_stream AS BOOLEAN) AS is_stream,
        TRY_CAST(is_download AS BOOLEAN) AS is_download,
        TRY_CAST(revenue AS DOUBLE) AS revenue,
        currency,
        country_code,
        genre_id,
        genre_name,
    FROM onerpm_raw
    WHERE quantity IS NOT NULL
      AND revenue IS NOT NULL
)

SELECT
    store,
    date,
    product,
    quantity,
    is_stream,
    is_download,
    revenue,
    currency,
    country_code,
    genre_id,
    genre_name
FROM source
