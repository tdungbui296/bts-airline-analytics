select
    flight_key,
    scheduled_elapsed_minutes,
    valid_scheduled_elapsed_minutes,
    is_eligible_for_scheduled_duration_metrics

from {{ ref('int_flights__quality_classified') }}

where
    valid_scheduled_elapsed_minutes is distinct from
        case
            when scheduled_elapsed_minutes >= 0
                then scheduled_elapsed_minutes
        end

    or is_eligible_for_scheduled_duration_metrics is distinct from
        coalesce(
            scheduled_elapsed_minutes >= 0,
            false
        )

    or valid_scheduled_elapsed_minutes < 0