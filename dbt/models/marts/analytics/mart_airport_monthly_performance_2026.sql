{{ config(
    materialized='view',
    enabled=var('enable_2026_reporting', false)
) }}

-- Reuses the historical mart's KPI definitions.
-- Reads only the verified 2026 incremental branch.
-- Does not modify the existing historical mart.
with departures as (

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

    from {{ ref('fact_flights_2026_monthly_incremental') }}

    group by
        flight_month,
        airport_id

),

arrivals as (

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

    from {{ ref('fact_flights_2026_monthly_incremental') }}

    group by
        flight_month,
        airport_id

),

airport_months as (

    select
        flight_month,
        airport_id

    from departures

    union distinct

    select
        flight_month,
        airport_id

    from arrivals

),

combined as (

    select
        airport_months.flight_month,
        airport_months.airport_id,

        coalesce(
            departures.scheduled_departures,
            0
        ) as scheduled_departures,

        coalesce(
            departures.operated_departures,
            0
        ) as operated_departures,

        coalesce(
            departures.cancelled_departures,
            0
        ) as cancelled_departures,

        coalesce(
            departures.departure_observed_flights,
            0
        ) as departure_observed_flights,

        coalesce(
            departures.departure_delayed_15_flights,
            0
        ) as departure_delayed_15_flights,

        coalesce(
            departures.departing_carrier_count,
            0
        ) as departing_carrier_count,

        coalesce(
            departures.destination_count,
            0
        ) as destination_count,

        coalesce(
            departures.total_departure_delay_minutes,
            0
        ) as total_departure_delay_minutes,

        coalesce(
            departures.taxi_out_observed_flights,
            0
        ) as taxi_out_observed_flights,

        coalesce(
            departures.total_taxi_out_minutes,
            0
        ) as total_taxi_out_minutes,

        coalesce(
            arrivals.scheduled_arrivals,
            0
        ) as scheduled_arrivals,

        coalesce(
            arrivals.diverted_inbound_flights,
            0
        ) as diverted_inbound_flights,

        coalesce(
            arrivals.arrival_observed_flights,
            0
        ) as arrival_observed_flights,

        coalesce(
            arrivals.arrival_delayed_15_flights,
            0
        ) as arrival_delayed_15_flights,

        coalesce(
            arrivals.arriving_carrier_count,
            0
        ) as arriving_carrier_count,

        coalesce(
            arrivals.origin_count,
            0
        ) as origin_count,

        coalesce(
            arrivals.total_arrival_delay_minutes,
            0
        ) as total_arrival_delay_minutes,

        coalesce(
            arrivals.taxi_in_observed_flights,
            0
        ) as taxi_in_observed_flights,

        coalesce(
            arrivals.total_taxi_in_minutes,
            0
        ) as total_taxi_in_minutes

    from airport_months

    left join departures
        on airport_months.flight_month
            = departures.flight_month
        and airport_months.airport_id
            = departures.airport_id

    left join arrivals
        on airport_months.flight_month
            = arrivals.flight_month
        and airport_months.airport_id
            = arrivals.airport_id

),

final as (

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

        safe_divide(
            cancelled_departures,
            scheduled_departures
        ) as departure_cancellation_rate,

        safe_divide(
            departure_delayed_15_flights,
            departure_observed_flights
        ) as departure_delay_15_rate,

        total_departure_delay_minutes,

        safe_divide(
            total_departure_delay_minutes,
            departure_observed_flights
        ) as avg_departure_delay_minutes,

        taxi_out_observed_flights,
        total_taxi_out_minutes,

        safe_divide(
            total_taxi_out_minutes,
            taxi_out_observed_flights
        ) as avg_taxi_out_minutes,

        scheduled_arrivals,
        diverted_inbound_flights,

        arrival_observed_flights,
        arrival_delayed_15_flights,

        arriving_carrier_count,
        origin_count,

        safe_divide(
            diverted_inbound_flights,
            scheduled_arrivals
        ) as inbound_diversion_rate,

        safe_divide(
            arrival_delayed_15_flights,
            arrival_observed_flights
        ) as arrival_delay_15_rate,

        1 - safe_divide(
            arrival_delayed_15_flights,
            arrival_observed_flights
        ) as on_time_arrival_rate,

        total_arrival_delay_minutes,

        safe_divide(
            total_arrival_delay_minutes,
            arrival_observed_flights
        ) as avg_arrival_delay_minutes,

        taxi_in_observed_flights,
        total_taxi_in_minutes,

        safe_divide(
            total_taxi_in_minutes,
            taxi_in_observed_flights
        ) as avg_taxi_in_minutes

    from combined

)

select *
from final