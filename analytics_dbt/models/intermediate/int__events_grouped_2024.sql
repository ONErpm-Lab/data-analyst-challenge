with

events_2024_q1 as (
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
        {{ ref('int__events_cleaned_2024_q1') }}

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

events_2024_q2 as (
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
        {{ ref('int__events_cleaned_2024_q2') }}

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

events_2024_m7 as (
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
        {{ ref('int__events_cleaned_2024_m7') }}

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

events_2024_m8 as (
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
        {{ ref('int__events_cleaned_2024_m8') }}

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

events_2024_m9 as (
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
        {{ ref('int__events_cleaned_2024_m9') }}

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

events_2024_m10 as (
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
        {{ ref('int__events_cleaned_2024_m10') }}

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

events_2024_m11 as (
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
        {{ ref('int__events_cleaned_2024_m11') }}

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

events_2024_m12 as (
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
        {{ ref('int__events_cleaned_2024_m12') }}

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
    select * from events_2024_q1
    union all
    select * from events_2024_q2
    union all
    select * from events_2024_m7
    union all
    select * from events_2024_m8
    union all
    select * from events_2024_m9
    union all
    select * from events_2024_m10
    union all
    select * from events_2024_m11
    union all
    select * from events_2024_m12
)

select * from final
