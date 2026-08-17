with expected as (

    select
        sum(
            case
                when carrier_delay_minutes > 0 then 1
                else 0
            end
            +
            case
                when weather_delay_minutes > 0 then 1
                else 0
            end
            +
            case
                when national_airspace_delay_minutes > 0 then 1
                else 0
            end
            +
            case
                when security_delay_minutes > 0 then 1
                else 0
            end
            +
            case
                when late_aircraft_delay_minutes > 0 then 1
                else 0
            end
        ) as expected_row_count

    from {{ ref('fact_flights') }}

    where is_eligible_for_delay_cause_metrics

),

actual as (

    select
        count(*) as actual_row_count

    from {{ ref('fact_flight_delay_cause') }}

)

select
    expected.expected_row_count,
    actual.actual_row_count

from expected
cross join actual

where expected.expected_row_count != actual.actual_row_count