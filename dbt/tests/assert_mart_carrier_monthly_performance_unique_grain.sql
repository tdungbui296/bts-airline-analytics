select
    flight_month,
    reporting_airline_dot_id,
    count(*) as row_count

from {{ ref('mart_carrier_monthly_performance') }}

group by
    flight_month,
    reporting_airline_dot_id

having count(*) > 1