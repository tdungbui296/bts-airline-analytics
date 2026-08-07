with staging_count as (

    select count(*) as row_count
    from {{ ref('stg_bts__flights') }}

),

intermediate_count as (

    select count(*) as row_count
    from {{ ref('int_flights__quality_classified') }}

)

select
    staging_count.row_count as staging_row_count,
    intermediate_count.row_count as intermediate_row_count

from staging_count
cross join intermediate_count

where staging_count.row_count
    != intermediate_count.row_count