with expected as (

    select
        *,

        safe_divide(
            cancelled_flights,
            scheduled_flights
        ) as expected_cancellation_rate,

        safe_divide(
            diverted_flights,
            scheduled_flights
        ) as expected_diversion_rate,

        safe_divide(
            departure_delayed_15_flights,
            departure_observed_flights
        ) as expected_departure_delay_15_rate,

        safe_divide(
            arrival_delayed_15_flights,
            arrival_observed_flights
        ) as expected_arrival_delay_15_rate,

        1 - safe_divide(
            arrival_delayed_15_flights,
            arrival_observed_flights
        ) as expected_on_time_arrival_rate,

        safe_divide(
            total_departure_delay_minutes,
            departure_observed_flights
        ) as expected_avg_departure_delay_minutes,

        safe_divide(
            total_arrival_delay_minutes,
            arrival_observed_flights
        ) as expected_avg_arrival_delay_minutes,

        safe_divide(
            total_scheduled_elapsed_minutes,
            scheduled_duration_eligible_flights
        ) as expected_avg_scheduled_elapsed_minutes,

        safe_divide(
            total_actual_elapsed_minutes,
            actual_duration_eligible_flights
        ) as expected_avg_actual_elapsed_minutes,

        safe_divide(
            total_air_time_minutes,
            air_time_observed_flights
        ) as expected_avg_air_time_minutes,

        safe_divide(
            total_taxi_out_minutes,
            taxi_out_observed_flights
        ) as expected_avg_taxi_out_minutes,

        safe_divide(
            total_taxi_in_minutes,
            taxi_in_observed_flights
        ) as expected_avg_taxi_in_minutes,

        safe_divide(
            total_distance_miles,
            distance_observed_flights
        ) as expected_avg_distance_miles

    from {{ ref('mart_carrier_monthly_performance') }}

)

select *

from expected

where cancellation_rate < 0
   or cancellation_rate > 1

   or diversion_rate < 0
   or diversion_rate > 1

   or (
       departure_delay_15_rate is not null
       and (
           departure_delay_15_rate < 0
           or departure_delay_15_rate > 1
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

   or cancellation_rate
      is distinct from expected_cancellation_rate

   or diversion_rate
      is distinct from expected_diversion_rate

   or departure_delay_15_rate
      is distinct from expected_departure_delay_15_rate

   or arrival_delay_15_rate
      is distinct from expected_arrival_delay_15_rate

   or on_time_arrival_rate
      is distinct from expected_on_time_arrival_rate

   or avg_departure_delay_minutes
      is distinct from expected_avg_departure_delay_minutes

   or avg_arrival_delay_minutes
      is distinct from expected_avg_arrival_delay_minutes

   or avg_scheduled_elapsed_minutes
      is distinct from expected_avg_scheduled_elapsed_minutes

   or avg_actual_elapsed_minutes
      is distinct from expected_avg_actual_elapsed_minutes

   or avg_air_time_minutes
      is distinct from expected_avg_air_time_minutes

   or avg_taxi_out_minutes
      is distinct from expected_avg_taxi_out_minutes

   or avg_taxi_in_minutes
      is distinct from expected_avg_taxi_in_minutes

   or avg_distance_miles
      is distinct from expected_avg_distance_miles