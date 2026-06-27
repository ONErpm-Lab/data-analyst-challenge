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

annual_revenue_by_country as (
    select
        country_code,
        currency,
        extract(year from date_of_event) as event_year,

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
        country_code,
        currency,
        extract(year from date_of_event)
),

year_over_year_by_country as (
    select
        current_year.country_code,
        current_year.currency,
        current_year.event_year,
        current_year.stream_quantity,
        current_year.download_quantity,
        current_year.total_quantity,
        current_year.reversal_quantity,
        current_year.gross_revenue,
        current_year.net_revenue,
        prior_year.net_revenue as prior_year_net_revenue,
        case
            when
                prior_year.net_revenue is not null
                and prior_year.net_revenue != 0
            then round(
                (current_year.net_revenue - prior_year.net_revenue)
                / prior_year.net_revenue * 100,
                2
            )
        end as year_over_year_growth_pct

    from annual_revenue_by_country as current_year

    left join annual_revenue_by_country as prior_year
        on current_year.event_year = prior_year.event_year + 1
        and current_year.country_code = prior_year.country_code
        and current_year.currency = prior_year.currency
),

avg_growth_by_country as (
    select
        country_code,
        currency,
        avg(year_over_year_growth_pct) as avg_growth_pct

    from year_over_year_by_country

    where year_over_year_growth_pct is not null

    group by
        country_code,
        currency
),

final as (
    select
        r.country_code,
        c.country_name,
        c.region,
        c.subregion,
        r.currency,
        r.event_year,
        r.stream_quantity,
        r.download_quantity,
        r.total_quantity,
        r.reversal_quantity,
        r.gross_revenue,
        r.net_revenue,
        r.prior_year_net_revenue,
        r.year_over_year_growth_pct,
        case
            when r.event_year = (select max(event_year) from annual_revenue_by_country)
            then round(r.net_revenue * (1 + g.avg_growth_pct / 100), 2)
        end as projected_next_year_net_revenue

    from year_over_year_by_country as r

    left join country_reference as c
        on r.country_code = c.country_code

    left join avg_growth_by_country as g
        on r.country_code = g.country_code
        and r.currency = g.currency
)

select * from final
