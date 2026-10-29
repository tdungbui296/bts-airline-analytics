{{ config(
    enabled=var('enable_incremental_demo', false),
    materialized='view'
) }}

{{ bts_staged_flights(source('raw_bts', 'flights_2026')) }}
