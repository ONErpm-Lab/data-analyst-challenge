with

events_2022_csv000 as (
    select * from {{ ref('stg__stg_2022_csv000') }}
),

events_2022_csv001 as (
    select * from {{ ref('stg__stg_2022_csv001') }}
),

events_2023_csv000 as (
    select * from {{ ref('stg__stg_2023_csv000') }}
),

events_2023_csv001 as (
    select * from {{ ref('stg__stg_2023_csv001') }}
),

events_2024_csv000 as (
    select * from {{ ref('stg__stg_2024_csv000') }}
),

events_2024_csv001 as (
    select * from {{ ref('stg__stg_2024_csv001') }}
),

events_2024_csv002 as (
    select * from {{ ref('stg__stg_2024_csv002') }}
),

events as (
    select * from events_2022_csv000
    union all
    select * from events_2022_csv001
    union all
    select * from events_2023_csv000
    union all
    select * from events_2023_csv001
    union all
    select * from events_2024_csv000
    union all
    select * from events_2024_csv001
    union all
    select * from events_2024_csv002
)

select * from events
