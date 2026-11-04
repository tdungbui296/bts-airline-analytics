

  create or replace view `tdb-dbt-bigquery-portfolio`.`dbt_dev`.`fact_flights_2026`
  OPTIONS()
  as 



select

    -- Grain and dimensional foreign keys
    flight_key,
    flight_date,
    reporting_airline_dot_id,
    origin_airport_id,
    destination_airport_id,

    -- Flight-level identifiers
    flight_number,
    tail_number,
    uses_fallback_flight_key,

    -- Departure operations
    scheduled_departure_hhmm,
    actual_departure_hhmm,
    departure_delay_minutes_signed,
    is_departure_delayed_15_minutes,
    taxi_out_minutes,
    wheels_off_hhmm,

    -- Arrival operations
    wheels_on_hhmm,
    taxi_in_minutes,
    scheduled_arrival_hhmm,
    actual_arrival_hhmm,
    arrival_delay_minutes_signed,
    is_arrival_delayed_15_minutes,

    -- Cancellation and diversion
    is_cancelled,
    operational_cancelled_flag,
    cancellation_code,
    is_diverted,
    valid_diversion_landing_count,
    is_cancelled_air_return,

    -- Duration and distance
    valid_scheduled_elapsed_minutes,
    actual_elapsed_minutes,
    air_time_minutes,
    distance_miles,

    -- Delay-cause measures
    carrier_delay_minutes,
    weather_delay_minutes,
    national_airspace_delay_minutes,
    security_delay_minutes,
    late_aircraft_delay_minutes,

    -- Analytical eligibility and important quality metadata
    has_negative_scheduled_elapsed_time,
    is_eligible_for_scheduled_duration_metrics,
    has_missing_delay_cause_breakdown,
    has_zero_delay_cause_breakdown,
    has_unclassified_incomplete_arrival,
    is_eligible_for_actual_duration_metrics,
    is_eligible_for_delay_cause_metrics

from `tdb-dbt-bigquery-portfolio`.`dbt_dev`.`int_flights__quality_classified_2026`

;

