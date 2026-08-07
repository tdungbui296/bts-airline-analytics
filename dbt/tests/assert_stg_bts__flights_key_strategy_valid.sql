select
    flight_key,
    flight_date,
    flight_number,
    tail_number,
    uses_fallback_flight_key

from {{ ref('stg_bts__flights') }}

where
    uses_fallback_flight_key is null

    or (
        flight_number is null
        and uses_fallback_flight_key = false
    )

    or (
        flight_number is not null
        and uses_fallback_flight_key = true
    )