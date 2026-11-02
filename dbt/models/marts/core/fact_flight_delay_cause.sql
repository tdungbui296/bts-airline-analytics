{{ config(
    materialized=var('historical_delay_cause_materialization', 'table'),
    cluster_by=[
        "origin_airport_id",
        "destination_airport_id",
        "reporting_airline_dot_id",
        "delay_cause_key"
    ]
) }}

with eligible_flights as (

    select
        flight_key,
        flight_date,
        reporting_airline_dot_id,
        origin_airport_id,
        destination_airport_id,

        carrier_delay_minutes,
        weather_delay_minutes,
        national_airspace_delay_minutes,
        security_delay_minutes,
        late_aircraft_delay_minutes

    from {{ ref('fact_flights') }}

    where is_eligible_for_delay_cause_metrics

),

delay_causes as (

    select
        eligible_flights.flight_key,
        eligible_flights.flight_date,
        eligible_flights.reporting_airline_dot_id,
        eligible_flights.origin_airport_id,
        eligible_flights.destination_airport_id,

        cause.delay_cause_key,
        cause.delay_minutes

    from eligible_flights

    cross join unnest([
        struct(
            'CARRIER' as delay_cause_key,
            carrier_delay_minutes as delay_minutes
        ),
        struct(
            'WEATHER' as delay_cause_key,
            weather_delay_minutes as delay_minutes
        ),
        struct(
            'NATIONAL_AIRSPACE' as delay_cause_key,
            national_airspace_delay_minutes as delay_minutes
        ),
        struct(
            'SECURITY' as delay_cause_key,
            security_delay_minutes as delay_minutes
        ),
        struct(
            'LATE_AIRCRAFT' as delay_cause_key,
            late_aircraft_delay_minutes as delay_minutes
        )
    ]) as cause

)

select
    flight_key,
    flight_date,
    reporting_airline_dot_id,
    origin_airport_id,
    destination_airport_id,
    delay_cause_key,
    delay_minutes

from delay_causes

where delay_minutes > 0