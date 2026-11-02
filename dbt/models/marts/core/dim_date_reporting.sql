{{ config(
    materialized='view',
    enabled=var('enable_2026_reporting', false)
) }}

-- Preserve the historical date dimension.
-- Append the complete 2026 calendar without overlap.
-- Extend the date range when future years are introduced.

with dates_2026 as (

    select
        date_day,
        extract(year from date_day) as calendar_year,
        extract(quarter from date_day) as calendar_quarter,
        extract(month from date_day) as calendar_month,
        format_date('%B', date_day) as month_name,
        format_date('%Y-%m', date_day) as year_month,
        extract(day from date_day) as day_of_month,
        mod(extract(dayofweek from date_day) + 5, 7) + 1
            as day_of_week_number,
        format_date('%A', date_day) as day_name,
        extract(isoweek from date_day) as iso_week,
        extract(isoyear from date_day) as iso_year,
        extract(dayofweek from date_day) in (1, 7)
            as is_weekend

    from unnest(
        generate_date_array(
            date '2026-01-01',
            date '2026-12-31'
        )
    ) as date_day

)

select *
from {{ ref('dim_date') }}
where date_day < date '2026-01-01'

union all by name

select *
from dates_2026