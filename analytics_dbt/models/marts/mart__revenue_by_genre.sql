with

all_events as (
    select * from {{ ref('int__events_final') }}
),

monthly_revenue_by_genre as (
    select
        genre_id,
        genre_name,
        date_of_event,
        currency,

        sum(quantity) filter (
            where is_stream and not is_reversal
        ) as stream_quantity,

        sum(quantity) filter (
            where is_download and not is_reversal
        ) as download_quantity,

        sum(quantity) filter (
            where not is_reversal
        ) as total_quantity,

        coalesce(sum(quantity) filter (
            where is_reversal
        ), 0) as reversal_quantity,

        sum(revenue) filter (
            where not is_reversal
        ) as gross_revenue,

        sum(revenue) as net_revenue

    from all_events

    group by
        genre_id,
        genre_name,
        date_of_event,
        currency
),

year_over_year_monthly_revenue_by_genre as (
    select
        current_month.genre_id,
        current_month.genre_name,
        current_month.date_of_event,
        current_month.currency,
        current_month.stream_quantity,
        current_month.download_quantity,
        current_month.total_quantity,
        current_month.reversal_quantity,
        current_month.gross_revenue,
        current_month.net_revenue,
        prior_month.net_revenue as prior_year_net_revenue,
        case
            when prior_month.net_revenue is not null
                and prior_month.net_revenue != 0
            then round(
                (current_month.net_revenue - prior_month.net_revenue)
                / prior_month.net_revenue * 100,
                2
            )
        end as year_over_year_growth_pct

    from monthly_revenue_by_genre as current_month

    left join monthly_revenue_by_genre as prior_month
        on current_month.date_of_event = prior_month.date_of_event + interval '1 year'
        and current_month.genre_id = prior_month.genre_id
        and current_month.currency = prior_month.currency
),

stream_to_revenue_conversion as (
    select
        genre_id,
        genre_name,
        date_of_event,
        currency,
        stream_quantity,
        download_quantity,
        total_quantity,
        reversal_quantity,
        gross_revenue,
        net_revenue,
        prior_year_net_revenue,
        year_over_year_growth_pct,
        case
            when stream_quantity > 0
                then net_revenue / stream_quantity
        end as net_revenue_per_stream

    from year_over_year_monthly_revenue_by_genre
),

final as (
    select
        genre_id,
        genre_name,
        date_of_event,
        currency,
        stream_quantity,
        download_quantity,
        total_quantity,
        reversal_quantity,
        gross_revenue,
        net_revenue,
        prior_year_net_revenue,
        year_over_year_growth_pct,
        net_revenue_per_stream,
        extract(year from date_of_event) as event_year,
        extract(month from date_of_event) as event_month

    from stream_to_revenue_conversion
)

select * from final
