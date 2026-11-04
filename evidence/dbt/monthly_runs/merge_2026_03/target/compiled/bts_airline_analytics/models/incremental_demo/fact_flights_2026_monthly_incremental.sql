







    
    




-- This model is separate from the full-history incremental demo.
-- Its physical target contains 2026 flights only.
--depends_on: `tdb-dbt-bigquery-portfolio`.`dbt_dev`.`fact_flights_2026`



    

    

        

        

    





    -- Select only the authorized month from the transformed source.
    -- The target-side predicates restrict MERGE matching to that month.
    select *
    from `tdb-dbt-bigquery-portfolio`.`dbt_dev`.`fact_flights_2026`
    where flight_date >= DATE('2026-03-01')
      and flight_date < DATE_ADD(
          DATE('2026-03-01'),
          INTERVAL 1 MONTH
      )

