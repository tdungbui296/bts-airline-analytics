select
    flight_month,
    airport_id,
    count(*) as row_count

from {{ ref('mart_airport_monthly_performance') }}

group by
    flight_month,
    airport_id

having count(*) > 1