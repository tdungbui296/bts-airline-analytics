select
    flight_key,
    delay_cause_key,
    count(*) as row_count

from {{ ref('fact_flight_delay_cause') }}

group by
    flight_key,
    delay_cause_key

having count(*) > 1