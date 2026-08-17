select
    flight_month,
    origin_airport_id,
    destination_airport_id,
    count(*) as row_count

from {{ ref('mart_route_monthly_performance') }}

group by
    flight_month,
    origin_airport_id,
    destination_airport_id

having count(*) > 1