{{ config(
    materialized='view',
    enabled=var('enable_2026_reporting', false)
) }}

-- Reuses the historical mart's KPI definitions.
-- Reads only the verified 2026 incremental branch.
-- Does not modify the existing historical mart.
with delay_causes as (

    select
        date_trunc(flight_date, month) as flight_month,
        reporting_airline_dot_id,
        delay_cause_key,
        flight_key,
        delay_minutes

    from {{ ref('fact_flight_delay_cause_2026') }}

),

cause_aggregated as (

    select
        flight_month,
        reporting_airline_dot_id,
        delay_cause_key,

        count(*) as affected_flights,

        sum(delay_minutes)
            as total_delay_minutes,

        avg(delay_minutes)
            as avg_delay_minutes_per_affected_flight,

        max(delay_minutes)
            as max_delay_minutes

    from delay_causes

    group by
        flight_month,
        reporting_airline_dot_id,
        delay_cause_key

),

carrier_month_totals as (

    select
        flight_month,
        reporting_airline_dot_id,

        sum(delay_minutes)
            as carrier_total_attributed_delay_minutes

    from delay_causes

    group by
        flight_month,
        reporting_airline_dot_id

),

final as (

    select
        cause_aggregated.flight_month,
        cause_aggregated.reporting_airline_dot_id,
        cause_aggregated.delay_cause_key,

        cause_aggregated.affected_flights,
        cause_aggregated.total_delay_minutes,
        cause_aggregated.avg_delay_minutes_per_affected_flight,
        cause_aggregated.max_delay_minutes,

        carrier_month_totals.carrier_total_attributed_delay_minutes,

        safe_divide(
            cause_aggregated.total_delay_minutes,
            carrier_month_totals.carrier_total_attributed_delay_minutes
        ) as cause_delay_minute_share_within_carrier_month

    from cause_aggregated

    inner join carrier_month_totals
        on cause_aggregated.flight_month
            = carrier_month_totals.flight_month
        and cause_aggregated.reporting_airline_dot_id
            = carrier_month_totals.reporting_airline_dot_id

)

select *
from final