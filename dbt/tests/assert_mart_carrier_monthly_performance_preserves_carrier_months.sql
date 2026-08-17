with expected as (

    select
        count(*) as expected_row_count

    from (

        select
            date_trunc(flight_date, month) as flight_month,
            reporting_airline_dot_id

        from {{ ref('fact_flights') }}

        group by
            flight_month,
            reporting_airline_dot_id

    )

),

actual as (

    select
        count(*) as actual_row_count

    from {{ ref('mart_carrier_monthly_performance') }}

)

select
    expected.expected_row_count,
    actual.actual_row_count

from expected
cross join actual

where expected.expected_row_count
    != actual.actual_row_count