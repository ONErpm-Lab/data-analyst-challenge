with

all_events as (
    select * from {{ ref('int__events_final') }}
),

country_reference as (
    select
        iso2 as country_code,
        name as country_name,
        region,
        subregion

    from {{ ref('countries') }}
),

monthly_revenue_by_country as (
    select
        e.country_code,
        e.date_of_event,
        e.currency,

        sum(e.quantity) filter (
            where e.is_stream and not e.is_reversal
        ) as stream_quantity,

        sum(e.quantity) filter (
            where e.is_download and not e.is_reversal
        ) as download_quantity,

        sum(e.quantity) filter (
            where not e.is_reversal
        ) as total_quantity,

        coalesce(sum(e.quantity) filter (
            where e.is_reversal
        ), 0) as reversal_quantity,

        sum(e.revenue) filter (
            where not e.is_reversal
        ) as gross_revenue,

        sum(e.revenue) as net_revenue

    from all_events as e

    group by
        e.country_code,
        e.date_of_event,
        e.currency
),

year_over_year_monthly_revenue_by_country as (
    select
        current_month.country_code,
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

    from monthly_revenue_by_country as current_month

    left join monthly_revenue_by_country as prior_month
        on current_month.date_of_event = prior_month.date_of_event + interval '1 year'
        and current_month.country_code = prior_month.country_code
        and current_month.currency = prior_month.currency
),

final as (
    select
        r.country_code,
        c.country_name,
        c.region,
        c.subregion,
        r.date_of_event,
        r.currency,
        r.stream_quantity,
        r.download_quantity,
        r.total_quantity,
        r.reversal_quantity,
        r.gross_revenue,
        r.net_revenue,
        r.prior_year_net_revenue,
        r.year_over_year_growth_pct,
        extract(year from r.date_of_event) as event_year,
        extract(month from r.date_of_event) as event_month

    from year_over_year_monthly_revenue_by_country as r

    left join country_reference as c
        on r.country_code = c.country_code
)

select * from final
