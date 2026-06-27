with

deduplicated as (
    select * from {{ ref('stg__stg_2023_csv000') }}

    where
        date_of_event between '2023-10-01' and '2023-12-31'

    union

    select * from {{ ref('stg__stg_2023_csv001') }}

    where
        date_of_event between '2023-10-01' and '2023-12-31'
),

reversal_classification as (
    select
        *,
        case
            when quantity < 0 then 1
            else 0
        end as is_reversal

    from
        deduplicated
),

fill_null_country_code as (
    select
        store,
        date_of_event,
        product,
        quantity,
        is_stream,
        is_download,
        revenue,
        currency,
        genre_id,
        genre_name,
        is_reversal::boolean as is_reversal,
        coalesce(country_code, 'NOT INFORMED') as country_code

    from
        reversal_classification
)

select * from fill_null_country_code
