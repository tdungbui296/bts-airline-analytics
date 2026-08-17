with expected_airport_months as (

    select
        date_trunc(flight_date, month) as flight_month,
        origin_airport_id as airport_id

    from {{ ref('fact_flights') }}

    group by
        flight_month,
        airport_id

    union distinct

    select
        date_trunc(flight_date, month) as flight_month,
        destination_airport_id as airport_id

    from {{ ref('fact_flights') }}

    group by
        flight_month,
        airport_id

),

expected as (

    select
        count(*) as expected_row_count

    from expected_airport_months

),

actual as (

    select
        count(*) as actual_row_count

    from {{ ref('mart_airport_monthly_performance') }}

)

select
    expected.expected_row_count,
    actual.actual_row_count

from expected
cross join actual

where expected.expected_row_count
    != actual.actual_row_count