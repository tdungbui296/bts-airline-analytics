{{ config(
    enabled=var('enable_incremental_demo', false),
    materialized='view'
) }}

{{ bts_fact_flights(ref('int_flights__quality_classified_2026')) }}
