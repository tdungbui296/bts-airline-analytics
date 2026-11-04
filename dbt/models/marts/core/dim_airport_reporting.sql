{{ config(materialized='view') }}

-- Historical airport dimension plus three verified Jan-Feb 2026 airports.
-- Evidence: evidence/bigquery/B3_51BI_airport_attributes_2026.json
-- This is a reporting extension, not a replacement for dim_airport.

with historical_airports as (

    select
        airport_id,
        airport_code,
        city_market_id,
        city_name,
        state_code
    from {{ ref('dim_airport') }}

),

additional_2026_airports as (

    select
        13121 as airport_id,
        'LWB' as airport_code,
        33121 as city_market_id,
        'Lewisburg, WV' as city_name,
        'WV' as state_code

    union all

    select
        14006,
        'PAH',
        34006,
        'Paducah, KY',
        'KY'

    union all

    select
        14802,
        'SHD',
        34802,
        'Staunton, VA',
        'VA'

)

select
    airport_id,
    airport_code,
    city_market_id,
    city_name,
    state_code
from historical_airports

union all

select
    airport_id,
    airport_code,
    city_market_id,
    city_name,
    state_code
from additional_2026_airports
