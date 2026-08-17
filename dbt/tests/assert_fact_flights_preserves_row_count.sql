with intermediate_count as (

    select
        count(*) as row_count

    from {{ ref('int_flights__quality_classified') }}

),

fact_count as (

    select
        count(*) as row_count

    from {{ ref('fact_flights') }}

),

validation as (

    select
        intermediate_count.row_count
            as intermediate_row_count,

        fact_count.row_count
            as fact_row_count

    from intermediate_count
    cross join fact_count

)

select *
from validation

where intermediate_row_count != fact_row_count