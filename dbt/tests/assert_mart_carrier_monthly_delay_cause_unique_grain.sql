select
    flight_month,
    reporting_airline_dot_id,
    delay_cause_key,
    count(*) as row_count

from {{ ref('mart_carrier_monthly_delay_cause') }}

group by
    flight_month,
    reporting_airline_dot_id,
    delay_cause_key

having count(*) > 1