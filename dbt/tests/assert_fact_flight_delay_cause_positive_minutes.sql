select *
from {{ ref('fact_flight_delay_cause') }}

where delay_minutes <= 0
   or delay_minutes is null