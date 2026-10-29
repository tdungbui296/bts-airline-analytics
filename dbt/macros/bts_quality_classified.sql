{% macro bts_quality_classified(staged_relation) %}

with flights as (

    select *
    from {{ staged_relation }}

),

quality_classified as (

    select
        flights.*,

        /*
        Keep only non-negative reported scheduled durations for
        analytical duration metrics. The original source value
        remains available as scheduled_elapsed_minutes.
        */
        case
            when scheduled_elapsed_minutes >= 0
                then scheduled_elapsed_minutes
        end as valid_scheduled_elapsed_minutes,

        coalesce(
            scheduled_elapsed_minutes >= 0,
            false
        ) as is_eligible_for_scheduled_duration_metrics,

        /*
        BTS code 9 is a valid categorical code for an air/fly
        return that ultimately ended in cancellation. It must
        never be interpreted as nine diversion landings.
        */
        coalesce(
            diversion_airport_landings = 9,
            false
        ) as is_cancelled_air_return,

        /*
        Preserve the original source cancellation fields while
        providing a business-facing interpretation of code 9.
        */
        (
            coalesce(is_cancelled, false)
            or coalesce(
                diversion_airport_landings = 9,
                false
            )
        ) as operational_cancelled_flag,

        /*
        Flag the 15 records where the source cancellation flag
        conflicts with the documented meaning of code 9.
        */
        coalesce(
            diversion_airport_landings = 9
            and not coalesce(is_cancelled, false),
            false
        ) as has_air_return_cancellation_conflict,

        /*
        Values 0 through 5 are numeric landing counts.
        Code 9 is excluded because it is categorical.

        A null value on a confirmed non-diverted flight is
        converted to zero only in this derived analytical field.
        The original field remains null.
        */
        case
            when diversion_airport_landings between 0 and 5
                then diversion_airport_landings

            when diversion_airport_landings is null
             and not coalesce(is_diverted, false)
                then 0
        end as valid_diversion_landing_count,

        coalesce(
            diversion_airport_landings is null
            and not coalesce(is_diverted, false),
            false
        ) as has_non_diverted_missing_landing_value,

        /*
        Overall delay status remains valid, but these records
        cannot be used in delay-cause decomposition.
        */
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
        ) as has_missing_delay_cause_breakdown,

        /*
        A qualifying delayed flight can also contain explicit zeroes
        across all five BTS delay-cause fields. Preserve this separately
        from the all-NULL missing-breakdown condition.
        */
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
        ) as has_zero_delay_cause_breakdown

    from flights

),

completion_classified as (

    select
        quality_classified.*,

        /*
        Flag flights that demonstrably departed and took off, are
        reported as neither operationally cancelled nor diverted,
        but lack final landing or actual-arrival evidence.

        These records are not treated as completed journeys for
        metrics requiring observed completion.
        */
        (
            not operational_cancelled_flag
            and not coalesce(is_diverted, false)
            and actual_departure_hhmm is not null
            and wheels_off_hhmm is not null
            and (
                wheels_on_hhmm is null
                or actual_arrival_hhmm is null
            )
        ) as has_unclassified_incomplete_arrival

    from quality_classified

),

metric_eligibility as (

    select
        completion_classified.*,

        /*
        Actual elapsed time is normally missing for cancelled
        flights because no completed gate-to-gate journey exists.
        */
        (
            coalesce(is_cancelled, false)
            and actual_elapsed_minutes is null
        ) as has_expected_cancelled_actual_elapsed_null,

        /*
        Scheduled duration normally remains available even when
        a flight is cancelled. Its absence is a source omission.
        */
        (
            coalesce(is_cancelled, false)
            and scheduled_elapsed_minutes is null
        ) as has_missing_scheduled_elapsed_on_cancelled_flight,

        /*
        Actual-duration metrics require a completed, operationally
        non-cancelled, non-diverted journey with an observed actual
        elapsed duration.
        */
        (
            not operational_cancelled_flag
            and not coalesce(is_diverted, false)
            and actual_elapsed_minutes is not null
            and not has_unclassified_incomplete_arrival
        ) as is_eligible_for_actual_duration_metrics,

        /*
        Delay-cause decomposition requires an eligible delayed,
        non-diverted flight with positive attributed minutes across
        the five BTS delay-cause categories.
        */
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
        ) as is_eligible_for_delay_cause_metrics

    from completion_classified

)

select *
from metric_eligibility

{% endmacro %}
