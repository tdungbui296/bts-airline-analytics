








-- This model is separate from the full-history incremental demo.
-- Its physical target contains 2026 flights only.
--depends_on: `tdb-dbt-bigquery-portfolio`.`dbt_dev`.`fact_flights_2026`





    -- Create the full 42-column table structure with zero rows.
    -- No historical fact data is duplicated.
    select *
    from `tdb-dbt-bigquery-portfolio`.`dbt_dev`.`fact_flights_2026`
    where false

