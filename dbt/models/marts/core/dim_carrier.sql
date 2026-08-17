select distinct
    reporting_airline_dot_id,
    reporting_airline_code

from {{ ref('stg_bts__flights') }}
