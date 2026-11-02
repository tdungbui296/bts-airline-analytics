{{ config(
    materialized='view',
    enabled=var('enable_2026_reporting', false)
) }}

-- Reporting bridge: preserve historical and incremental facts.
-- No data is copied by this model until explicitly deployed.
-- BY NAME protects against positional column mismatches.

select *
from {{ ref('fact_flights') }}
where flight_date >= date '2024-01-01'
  and flight_date < date '2026-01-01'

union all by name

select *
from {{ ref('fact_flights_2026_monthly_incremental') }}
where flight_date >= date '2026-01-01'