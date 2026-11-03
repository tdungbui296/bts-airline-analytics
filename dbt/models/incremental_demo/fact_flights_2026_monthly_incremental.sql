{% set run_mode = var('incremental_2026_mode', none) %}
{% set selected_month = var('reprocess_month', none) %}
{# Validate a canonical YYYY-MM month within the 2026 source contract. #}
{% set valid_month_numbers = [
    '01', '02', '03', '04', '05', '06',
    '07', '08', '09', '10', '11', '12'
] %}
{% set month_is_valid = (
    selected_month is string
    and selected_month|length == 7
    and selected_month[:5] == '2026-'
    and selected_month[5:] in valid_month_numbers
) %}
{% set target_predicates = [] %}

{% if run_mode == 'monthly' and month_is_valid %}
    {% set month_start = selected_month ~ '-01' %}
    {% set target_predicates = [
        "DBT_INTERNAL_DEST.flight_date >= DATE('" ~ month_start ~ "')",
        "DBT_INTERNAL_DEST.flight_date < DATE_ADD(DATE('" ~ month_start ~ "'), INTERVAL 1 MONTH)"
    ] %}
{% endif %}

{{ config(
    enabled=var('enable_2026_incremental_demo', false),
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
    ],
    incremental_predicates=target_predicates
) }}

-- This model is separate from the full-history incremental demo.
-- Its physical target contains 2026 flights only.
--depends_on: {{ ref('fact_flights_2026') }}

{% if execute and flags.WHICH in ['run', 'build'] %}

    {% if flags.FULL_REFRESH %}
        {{ exceptions.raise_compiler_error(
            "Full refresh is prohibited for the 2026 incremental demo."
        ) }}
    {% endif %}

    {% if run_mode == 'bootstrap' %}

        {% if selected_month is not none %}
            {{ exceptions.raise_compiler_error(
                "Bootstrap must not specify reprocess_month."
            ) }}
        {% endif %}

        {% if is_incremental() %}
            {{ exceptions.raise_compiler_error(
                "Bootstrap blocked: target table already exists."
            ) }}
        {% endif %}

    {% elif run_mode == 'monthly' %}

        {% if not month_is_valid %}
            {{ exceptions.raise_compiler_error(
                "Expected a valid YYYY-MM month within the 2026 source contract."
            ) }}
        {% endif %}

        {% if not is_incremental() %}
            {{ exceptions.raise_compiler_error(
                "Monthly MERGE blocked: bootstrap target first."
            ) }}
        {% endif %}

    {% else %}

        {{ exceptions.raise_compiler_error(
            "Set incremental_2026_mode to bootstrap or monthly."
        ) }}

    {% endif %}

{% endif %}

{% if run_mode == 'bootstrap' %}

    -- Create the full 42-column table structure with zero rows.
    -- No historical fact data is duplicated.
    select *
    from {{ ref('fact_flights_2026') }}
    where false

{% elif run_mode == 'monthly' and month_is_valid %}

    -- Select only the authorized month from the transformed source.
    -- The target-side predicates restrict MERGE matching to that month.
    select *
    from {{ ref('fact_flights_2026') }}
    where flight_date >= DATE('{{ selected_month }}-01')
      and flight_date < DATE_ADD(
          DATE('{{ selected_month }}-01'),
          INTERVAL 1 MONTH
      )

{% else %}

    -- Safe placeholder for dbt parsing.
    select *
    from {{ ref('fact_flights_2026') }}
    where false

{% endif %}