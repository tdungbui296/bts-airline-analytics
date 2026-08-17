with expected_cause as (

    select
        date_trunc(flight_date, month) as flight_month,
        reporting_airline_dot_id,
        delay_cause_key,

        count(*) as expected_affected_flights,

        sum(delay_minutes)
            as expected_total_delay_minutes,

        avg(delay_minutes)
            as expected_avg_delay_minutes,

        max(delay_minutes)
            as expected_max_delay_minutes

    from {{ ref('fact_flight_delay_cause') }}

    group by
        flight_month,
        reporting_airline_dot_id,
        delay_cause_key

),

expected_carrier_month as (

    select
        date_trunc(flight_date, month) as flight_month,
        reporting_airline_dot_id,

        sum(delay_minutes)
            as expected_carrier_total_delay_minutes

    from {{ ref('fact_flight_delay_cause') }}

    group by
        flight_month,
        reporting_airline_dot_id

),

expected as (

    select
        expected_cause.flight_month,
        expected_cause.reporting_airline_dot_id,
        expected_cause.delay_cause_key,

        expected_cause.expected_affected_flights,
        expected_cause.expected_total_delay_minutes,
        expected_cause.expected_avg_delay_minutes,
        expected_cause.expected_max_delay_minutes,

        expected_carrier_month.expected_carrier_total_delay_minutes

    from expected_cause

    inner join expected_carrier_month
        on expected_cause.flight_month
            = expected_carrier_month.flight_month
        and expected_cause.reporting_airline_dot_id
            = expected_carrier_month.reporting_airline_dot_id

),

actual as (

    select *
    from {{ ref('mart_carrier_monthly_delay_cause') }}

)

select
    expected.flight_month,
    expected.reporting_airline_dot_id,
    expected.delay_cause_key

from expected

left join actual
    on expected.flight_month = actual.flight_month
    and expected.reporting_airline_dot_id
        = actual.reporting_airline_dot_id
    and expected.delay_cause_key
        = actual.delay_cause_key

where actual.delay_cause_key is null

   or expected.expected_affected_flights
      != actual.affected_flights

   or expected.expected_total_delay_minutes
      != actual.total_delay_minutes

   or expected.expected_max_delay_minutes
      != actual.max_delay_minutes

   or expected.expected_carrier_total_delay_minutes
      != actual.carrier_total_attributed_delay_minutes

   or abs(
       expected.expected_avg_delay_minutes
       - actual.avg_delay_minutes_per_affected_flight
   ) > 0.000000001