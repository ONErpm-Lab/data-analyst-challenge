with

events_2023_q1 as (
    select
        store,
        is_stream,
        is_download,
        currency,
        genre_id,
        genre_name,
        country_code,
        is_reversal,
        date_trunc('month', date_of_event) as date_of_event,
        sum(quantity) as quantity,
        sum(revenue) as revenue

    from
        {{ ref('int__events_cleaned_2023_q1') }}

    group by
        store,
        is_stream,
        is_download,
        currency,
        genre_id,
        genre_name,
        country_code,
        is_reversal,
        date_trunc('month', date_of_event)
),

events_2023_q2 as (
    select
        store,
        is_stream,
        is_download,
        currency,
        genre_id,
        genre_name,
        country_code,
        is_reversal,
        date_trunc('month', date_of_event) as date_of_event,
        sum(quantity) as quantity,
        sum(revenue) as revenue

    from
        {{ ref('int__events_cleaned_2023_q2') }}

    group by
        store,
        is_stream,
        is_download,
        currency,
        genre_id,
        genre_name,
        country_code,
        is_reversal,
        date_trunc('month', date_of_event)
),

events_2023_q3 as (
    select
        store,
        is_stream,
        is_download,
        currency,
        genre_id,
        genre_name,
        country_code,
        is_reversal,
        date_trunc('month', date_of_event) as date_of_event,
        sum(quantity) as quantity,
        sum(revenue) as revenue

    from
        {{ ref('int__events_cleaned_2023_q3') }}

    group by
        store,
        is_stream,
        is_download,
        currency,
        genre_id,
        genre_name,
        country_code,
        is_reversal,
        date_trunc('month', date_of_event)
),

events_2023_q4 as (
    select
        store,
        is_stream,
        is_download,
        currency,
        genre_id,
        genre_name,
        country_code,
        is_reversal,
        date_trunc('month', date_of_event) as date_of_event,
        sum(quantity) as quantity,
        sum(revenue) as revenue

    from
        {{ ref('int__events_cleaned_2023_q4') }}

    group by
        store,
        is_stream,
        is_download,
        currency,
        genre_id,
        genre_name,
        country_code,
        is_reversal,
        date_trunc('month', date_of_event)
),

final as (
    select * from events_2023_q1
    union all
    select * from events_2023_q2
    union all
    select * from events_2023_q3
    union all
    select * from events_2023_q4
)

select * from final
