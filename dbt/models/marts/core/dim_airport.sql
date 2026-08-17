with origin_airports as (

    select
        origin_airport_id as airport_id,
        origin_airport_code as airport_code,
        origin_city_market_id as city_market_id,
        origin_city_name as city_name,
        origin_state_code as state_code

    from {{ ref('stg_bts__flights') }}

    where origin_airport_id is not null

),

destination_airports as (

    select
        destination_airport_id as airport_id,
        destination_airport_code as airport_code,
        destination_city_market_id as city_market_id,
        destination_city_name as city_name,
        destination_state_code as state_code

    from {{ ref('stg_bts__flights') }}

    where destination_airport_id is not null

),

combined_airports as (

    select * from origin_airports

    union all

    select * from destination_airports

)

select distinct
    airport_id,
    airport_code,
    city_market_id,
    city_name,
    state_code

from combined_airports
