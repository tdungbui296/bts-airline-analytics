with expected as (

    select
        *,

        safe_divide(
            cancelled_departures,
            scheduled_departures
        ) as expected_departure_cancellation_rate,

        safe_divide(
            departure_delayed_15_flights,
            departure_observed_flights
        ) as expected_departure_delay_15_rate,

        safe_divide(
            total_departure_delay_minutes,
            departure_observed_flights
        ) as expected_avg_departure_delay_minutes,

        safe_divide(
            total_taxi_out_minutes,
            taxi_out_observed_flights
        ) as expected_avg_taxi_out_minutes,

        safe_divide(
            diverted_inbound_flights,
            scheduled_arrivals
        ) as expected_inbound_diversion_rate,

        safe_divide(
            arrival_delayed_15_flights,
            arrival_observed_flights
        ) as expected_arrival_delay_15_rate,

        1 - safe_divide(
            arrival_delayed_15_flights,
            arrival_observed_flights
        ) as expected_on_time_arrival_rate,

        safe_divide(
            total_arrival_delay_minutes,
            arrival_observed_flights
        ) as expected_avg_arrival_delay_minutes,

        safe_divide(
            total_taxi_in_minutes,
            taxi_in_observed_flights
        ) as expected_avg_taxi_in_minutes

    from {{ ref('mart_airport_monthly_performance') }}

)

select *

from expected

where (
        departure_cancellation_rate is not null
        and (
            departure_cancellation_rate < 0
            or departure_cancellation_rate > 1
        )
    )

   or (
        departure_delay_15_rate is not null
        and (
            departure_delay_15_rate < 0
            or departure_delay_15_rate > 1
        )
    )

   or (
        inbound_diversion_rate is not null
        and (
            inbound_diversion_rate < 0
            or inbound_diversion_rate > 1
        )
    )

   or (
        arrival_delay_15_rate is not null
        and (
            arrival_delay_15_rate < 0
            or arrival_delay_15_rate > 1
        )
    )

   or (
        on_time_arrival_rate is not null
        and (
            on_time_arrival_rate < 0
            or on_time_arrival_rate > 1
        )
    )

   or departure_cancellation_rate
      is distinct from expected_departure_cancellation_rate

   or departure_delay_15_rate
      is distinct from expected_departure_delay_15_rate

   or avg_departure_delay_minutes
      is distinct from expected_avg_departure_delay_minutes

   or avg_taxi_out_minutes
      is distinct from expected_avg_taxi_out_minutes

   or inbound_diversion_rate
      is distinct from expected_inbound_diversion_rate

   or arrival_delay_15_rate
      is distinct from expected_arrival_delay_15_rate

   or on_time_arrival_rate
      is distinct from expected_on_time_arrival_rate

   or avg_arrival_delay_minutes
      is distinct from expected_avg_arrival_delay_minutes

   or avg_taxi_in_minutes
      is distinct from expected_avg_taxi_in_minutes