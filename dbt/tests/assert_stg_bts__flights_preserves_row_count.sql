with raw_counts as (

    select count(*) as row_count
    from {{ source('raw_bts', 'flights') }}

),

staging_counts as (

    select count(*) as row_count
    from {{ ref('stg_bts__flights') }}

)

select
    raw_counts.row_count as raw_row_count,
    staging_counts.row_count as staging_row_count

from raw_counts
cross join staging_counts

where raw_counts.row_count != staging_counts.row_count