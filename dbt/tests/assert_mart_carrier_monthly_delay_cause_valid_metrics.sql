select *

from {{ ref('mart_carrier_monthly_delay_cause') }}

where affected_flights <= 0

   or total_delay_minutes <= 0

   or avg_delay_minutes_per_affected_flight <= 0

   or max_delay_minutes <= 0

   or carrier_total_attributed_delay_minutes <= 0

   or total_delay_minutes
      > carrier_total_attributed_delay_minutes

   or avg_delay_minutes_per_affected_flight
      > max_delay_minutes

   or cause_delay_minute_share_within_carrier_month <= 0

   or cause_delay_minute_share_within_carrier_month > 1