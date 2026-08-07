select
    flight_key

from {{ ref('int_flights__quality_classified') }}

where
    is_cancelled_air_return is distinct from
        coalesce(
            diversion_airport_landings = 9,
            false
        )

    or operational_cancelled_flag is distinct from
        (
            coalesce(is_cancelled, false)
            or coalesce(
                diversion_airport_landings = 9,
                false
            )
        )

    or has_air_return_cancellation_conflict is distinct from
        coalesce(
            diversion_airport_landings = 9
            and not coalesce(is_cancelled, false),
            false
        )

    or valid_diversion_landing_count is distinct from
        case
            when diversion_airport_landings between 0 and 5
                then diversion_airport_landings

            when diversion_airport_landings is null
             and not coalesce(is_diverted, false)
                then 0
        end

    or has_non_diverted_missing_landing_value is distinct from
        coalesce(
            diversion_airport_landings is null
            and not coalesce(is_diverted, false),
            false
        )

or has_unclassified_incomplete_arrival is distinct from
    (
        not operational_cancelled_flag
        and not coalesce(is_diverted, false)
        and actual_departure_hhmm is not null
        and wheels_off_hhmm is not null
        and (
            wheels_on_hhmm is null
            or actual_arrival_hhmm is null
        )
    )
