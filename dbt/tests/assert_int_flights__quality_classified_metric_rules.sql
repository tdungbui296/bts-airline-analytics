select
    flight_key

from {{ ref('int_flights__quality_classified') }}

where
    has_expected_cancelled_actual_elapsed_null is distinct from
    (
        coalesce(is_cancelled, false)
        and actual_elapsed_minutes is null
    )

    or has_missing_scheduled_elapsed_on_cancelled_flight
        is distinct from
        (
            coalesce(is_cancelled, false)
            and scheduled_elapsed_minutes is null
        )

    or has_missing_delay_cause_breakdown is distinct from
        (
            coalesce(
                is_arrival_delayed_15_minutes,
                false
            )
            and not coalesce(is_diverted, false)
            and carrier_delay_minutes is null
            and weather_delay_minutes is null
            and national_airspace_delay_minutes is null
            and security_delay_minutes is null
            and late_aircraft_delay_minutes is null
        )

    or has_zero_delay_cause_breakdown is distinct from
        coalesce(
            (
                coalesce(
                    is_arrival_delayed_15_minutes,
                    false
                )
                and not coalesce(is_diverted, false)
                and carrier_delay_minutes = 0
                and weather_delay_minutes = 0
                and national_airspace_delay_minutes = 0
                and security_delay_minutes = 0
                and late_aircraft_delay_minutes = 0
            ),
            false
        )

    or is_eligible_for_actual_duration_metrics is distinct from
        (
            not operational_cancelled_flag
            and not coalesce(is_diverted, false)
            and actual_elapsed_minutes is not null
        )

    or is_eligible_for_delay_cause_metrics is distinct from
        (
            coalesce(
                is_arrival_delayed_15_minutes,
                false
            )
            and not coalesce(is_diverted, false)
            and (
                coalesce(carrier_delay_minutes, 0)
                + coalesce(weather_delay_minutes, 0)
                + coalesce(national_airspace_delay_minutes, 0)
                + coalesce(security_delay_minutes, 0)
                + coalesce(late_aircraft_delay_minutes, 0)
            ) > 0
        )

    or (
        is_eligible_for_delay_cause_metrics
        and (
            coalesce(carrier_delay_minutes, 0)
            + coalesce(weather_delay_minutes, 0)
            + coalesce(national_airspace_delay_minutes, 0)
            + coalesce(security_delay_minutes, 0)
            + coalesce(late_aircraft_delay_minutes, 0)
        ) is distinct from arrival_delay_minutes
    )
