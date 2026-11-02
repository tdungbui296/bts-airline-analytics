{{ config(
    materialized='view',
    enabled=var('enable_2026_reporting', false)
) }}

-- Unified monthly reporting view.
-- Historical materialized marts remain unchanged.
-- Monthly date boundaries prevent overlapping periods.
-- BY NAME guards against positional column mismatches.

select *
from {{ ref('mart_airport_monthly_performance') }}
where flight_month < date '2026-01-01'

union all by name

select *
from {{ ref('mart_airport_monthly_performance_2026') }}
where flight_month >= date '2026-01-01'
  and flight_month < date '2027-01-01'