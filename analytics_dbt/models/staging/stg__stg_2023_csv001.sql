with

source as (
    select *

    from {{ source('raw', 'raw_2023_csv001') }}
),

renamed as (
    select
        column00 as store,
        column01 as date_of_event, --not using date as name (keyword)
        column02 as product,
        column03 as quantity,
        column04 as is_stream,
        column05 as is_download,
        column06 as revenue,
        column07 as currency,
        column08 as country_code,
        column09 as genre_id,
        column10 as genre_name

    from source
)

select * from renamed
