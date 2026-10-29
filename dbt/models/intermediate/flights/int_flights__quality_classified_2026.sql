{{ config(
    enabled=var('enable_incremental_demo', false),
    materialized='view'
) }}

{{ bts_quality_classified(ref('stg_bts__flights_2026')) }}
