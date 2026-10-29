{{ config(
    enabled=var('enable_incremental_demo', false),
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='flight_key',
    on_schema_change='fail',
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

-- Register both dependencies even if a conditional branch is not parsed.
--depends_on: {{ ref('fact_flights') }}
--depends_on: {{ ref('fact_flights_2026') }}

{# Two explicit stages: build historical table once, then monthly MERGE. #}
{% set load_mode = var('incremental_load_mode', none) %}
{% set selected_month = var('reprocess_month', none) %}
{% set valid_months = ['01','02','03','04','05','06',
                       '07','08','09','10','11','12'] %}

{# Parse-time must remain permissive so `dbt ls` and `dbt parse` work. #}
{% if execute and load_mode not in ['bootstrap', 'monthly'] %}
    {{ exceptions.raise_compiler_error(
        "Set incremental_load_mode explicitly to 'bootstrap' or 'monthly'."
    ) }}
{% endif %}

{% if load_mode == 'bootstrap' %}

    {% if execute and selected_month is not none %}
        {{ exceptions.raise_compiler_error(
            "Bootstrap must not include reprocess_month."
        ) }}
    {% endif %}

    {% if execute and flags.WHICH in ['run', 'build'] %}
        {% if flags.FULL_REFRESH %}
            {{ exceptions.raise_compiler_error(
                "--full-refresh is prohibited for this incremental demo."
            ) }}
        {% endif %}
        {% if is_incremental() %}
            {{ exceptions.raise_compiler_error(
                "Bootstrap blocked: the incremental target already exists. "
                ~ "Use incremental_load_mode: monthly instead."
            ) }}
        {% endif %}
    {% endif %}

    -- Initial table: ALL validated 2024-2025 flights, not 2026.
    select *
    from {{ ref('fact_flights') }}

{% elif load_mode == 'monthly' %}

    {% if selected_month is not string
          or selected_month|length != 7
          or selected_month[:5] != '2026-'
          or selected_month[5:] not in valid_months %}
        {{ exceptions.raise_compiler_error(
            "reprocess_month must be a quoted YYYY-MM string within 2026, "
            ~ "for example '2026-01'."
        ) }}
    {% endif %}

    {% if execute and flags.WHICH in ['run', 'build'] %}
        {% if flags.FULL_REFRESH %}
            {{ exceptions.raise_compiler_error(
                "--full-refresh is prohibited for monthly MERGE."
            ) }}
        {% endif %}
        {% if not is_incremental() %}
            {{ exceptions.raise_compiler_error(
                "Monthly MERGE blocked: the target incremental table does not exist. "
                ~ "First bootstrap the full 2024-2025 baseline."
            ) }}
        {% endif %}
    {% endif %}

    -- Only the requested source month enters the MERGE.
    -- Non-key corrections for this month update matching flight_key rows.
    select *
    from {{ ref('fact_flights_2026') }}
    where flight_date >= date('{{ selected_month }}-01')
      and flight_date < date_add(date('{{ selected_month }}-01'), interval 1 month)

{% else %}

    -- Parse-only placeholder; during compilation an explicit mode is mandatory.
    select *
    from {{ ref('fact_flights') }}
    where false

{% endif %}
