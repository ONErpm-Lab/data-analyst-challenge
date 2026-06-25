select * from {{ ref('int__events_grouped_2022') }}

union all

select * from {{ ref('int__events_grouped_2023') }}

union all

select * from {{ ref('int__events_grouped_2024') }}
