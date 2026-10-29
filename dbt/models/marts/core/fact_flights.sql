{{ config(
    materialized='view',
    partition_by={
        "field": "flight_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=[
        "origin_airport_id",
        "destination_airport_id",
        "reporting_airline_dot_id"
    ]
) }}

{{ bts_fact_flights(ref('int_flights__quality_classified')) }}
