select
    flight_month,
    reporting_airline_dot_id,
    sum(
        cause_delay_minute_share_within_carrier_month
    ) as cause_share_sum

from {{ ref('mart_carrier_monthly_delay_cause') }}

group by
    flight_month,
    reporting_airline_dot_id

having abs(
    sum(
        cause_delay_minute_share_within_carrier_month
    ) - 1
) > 0.000000001