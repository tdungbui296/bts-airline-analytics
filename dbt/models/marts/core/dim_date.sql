with date_bounds as (

    select
        min(flight_date) as min_date,
        max(flight_date) as max_date

    from {{ ref('stg_bts__flights') }}

),

date_spine as (

    select
        date_day

    from date_bounds,
    unnest(generate_date_array(min_date, max_date)) as date_day

)

select
    date_day,
    extract(year from date_day) as calendar_year,
    extract(quarter from date_day) as calendar_quarter,
    extract(month from date_day) as calendar_month,
    format_date('%B', date_day) as month_name,
    format_date('%Y-%m', date_day) as year_month,
    extract(day from date_day) as day_of_month,
    mod(extract(dayofweek from date_day) + 5, 7) + 1 as day_of_week_number,
    format_date('%A', date_day) as day_name,
    extract(isoweek from date_day) as iso_week,
    extract(isoyear from date_day) as iso_year,
    extract(dayofweek from date_day) in (1, 7) as is_weekend

from date_spine
