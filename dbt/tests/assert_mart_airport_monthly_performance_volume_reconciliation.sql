with expected_departures as (

    select
        date_trunc(flight_date, month) as flight_month,
        origin_airport_id as airport_id,

        count(*) as scheduled_departures,

        countif(
            not operational_cancelled_flag
        ) as operated_departures,

        countif(
            operational_cancelled_flag
        ) as cancelled_departures,

        countif(
            departure_delay_minutes_signed is not null
        ) as departure_observed_flights,

        countif(
            is_departure_delayed_15_minutes
        ) as departure_delayed_15_flights,

        count(distinct reporting_airline_dot_id)
            as departing_carrier_count,

        count(distinct destination_airport_id)
            as destination_count,

        sum(
            coalesce(
                greatest(departure_delay_minutes_signed, 0),
                0
            )
        ) as total_departure_delay_minutes,

        countif(
            taxi_out_minutes is not null
        ) as taxi_out_observed_flights,

        sum(
            coalesce(taxi_out_minutes, 0)
        ) as total_taxi_out_minutes

    from {{ ref('fact_flights') }}

    group by
        flight_month,
        airport_id

),

expected_arrivals as (

    select
        date_trunc(flight_date, month) as flight_month,
        destination_airport_id as airport_id,

        count(*) as scheduled_arrivals,

        countif(
            is_diverted
        ) as diverted_inbound_flights,

        countif(
            arrival_delay_minutes_signed is not null
        ) as arrival_observed_flights,

        countif(
            is_arrival_delayed_15_minutes
        ) as arrival_delayed_15_flights,

        count(distinct reporting_airline_dot_id)
            as arriving_carrier_count,

        count(distinct origin_airport_id)
            as origin_count,

        sum(
            coalesce(
                greatest(arrival_delay_minutes_signed, 0),
                0
            )
        ) as total_arrival_delay_minutes,

        countif(
            taxi_in_minutes is not null
        ) as taxi_in_observed_flights,

        sum(
            coalesce(taxi_in_minutes, 0)
        ) as total_taxi_in_minutes

    from {{ ref('fact_flights') }}

    group by
        flight_month,
        airport_id

),

expected_airport_months as (

    select
        flight_month,
        airport_id

    from expected_departures

    union distinct

    select
        flight_month,
        airport_id

    from expected_arrivals

),

expected as (

    select
        expected_airport_months.flight_month,
        expected_airport_months.airport_id,

        coalesce(
            expected_departures.scheduled_departures,
            0
        ) as scheduled_departures,

        coalesce(
            expected_departures.operated_departures,
            0
        ) as operated_departures,

        coalesce(
            expected_departures.cancelled_departures,
            0
        ) as cancelled_departures,

        coalesce(
            expected_departures.departure_observed_flights,
            0
        ) as departure_observed_flights,

        coalesce(
            expected_departures.departure_delayed_15_flights,
            0
        ) as departure_delayed_15_flights,

        coalesce(
            expected_departures.departing_carrier_count,
            0
        ) as departing_carrier_count,

        coalesce(
            expected_departures.destination_count,
            0
        ) as destination_count,

        coalesce(
            expected_departures.total_departure_delay_minutes,
            0
        ) as total_departure_delay_minutes,

        coalesce(
            expected_departures.taxi_out_observed_flights,
            0
        ) as taxi_out_observed_flights,

        coalesce(
            expected_departures.total_taxi_out_minutes,
            0
        ) as total_taxi_out_minutes,

        coalesce(
            expected_arrivals.scheduled_arrivals,
            0
        ) as scheduled_arrivals,

        coalesce(
            expected_arrivals.diverted_inbound_flights,
            0
        ) as diverted_inbound_flights,

        coalesce(
            expected_arrivals.arrival_observed_flights,
            0
        ) as arrival_observed_flights,

        coalesce(
            expected_arrivals.arrival_delayed_15_flights,
            0
        ) as arrival_delayed_15_flights,

        coalesce(
            expected_arrivals.arriving_carrier_count,
            0
        ) as arriving_carrier_count,

        coalesce(
            expected_arrivals.origin_count,
            0
        ) as origin_count,

        coalesce(
            expected_arrivals.total_arrival_delay_minutes,
            0
        ) as total_arrival_delay_minutes,

        coalesce(
            expected_arrivals.taxi_in_observed_flights,
            0
        ) as taxi_in_observed_flights,

        coalesce(
            expected_arrivals.total_taxi_in_minutes,
            0
        ) as total_taxi_in_minutes

    from expected_airport_months

    left join expected_departures
        on expected_airport_months.flight_month
            = expected_departures.flight_month
        and expected_airport_months.airport_id
            = expected_departures.airport_id

    left join expected_arrivals
        on expected_airport_months.flight_month
            = expected_arrivals.flight_month
        and expected_airport_months.airport_id
            = expected_arrivals.airport_id

),

actual as (

    select
        flight_month,
        airport_id,

        scheduled_departures,
        operated_departures,
        cancelled_departures,

        departure_observed_flights,
        departure_delayed_15_flights,

        departing_carrier_count,
        destination_count,

        total_departure_delay_minutes,
        taxi_out_observed_flights,
        total_taxi_out_minutes,

        scheduled_arrivals,
        diverted_inbound_flights,

        arrival_observed_flights,
        arrival_delayed_15_flights,

        arriving_carrier_count,
        origin_count,

        total_arrival_delay_minutes,
        taxi_in_observed_flights,
        total_taxi_in_minutes

    from {{ ref('mart_airport_monthly_performance') }}

),

differences as (

    (
        select *
        from expected

        except distinct

        select *
        from actual
    )

    union all

    (
        select *
        from actual

        except distinct

        select *
        from expected
    )

)

select *
from differences