SELECT 'fact_unpivot' AS source,
       DATE_TRUNC(f.flight_date, MONTH) AS flight_month,
       COUNT(*) AS affected_cause_rows,
       SUM(delay_minutes) AS attributed_minutes
FROM `tdb-dbt-bigquery-portfolio.dbt_dev.fact_flights_2026_monthly_incremental` AS f
CROSS JOIN UNNEST([
    SAFE_CAST(f.carrier_delay_minutes AS NUMERIC),
    SAFE_CAST(f.weather_delay_minutes AS NUMERIC),
    SAFE_CAST(f.national_airspace_delay_minutes AS NUMERIC),
    SAFE_CAST(f.security_delay_minutes AS NUMERIC),
    SAFE_CAST(f.late_aircraft_delay_minutes AS NUMERIC)
]) AS delay_minutes
WHERE f.flight_date >= DATE '2026-01-01'
  AND f.flight_date < DATE '2026-03-01'
  AND f.is_eligible_for_delay_cause_metrics = TRUE
  AND delay_minutes > 0
GROUP BY 2

UNION ALL

SELECT 'delay_view', DATE_TRUNC(flight_date, MONTH),
       COUNT(*),
       SUM(CAST(delay_minutes AS NUMERIC))
FROM `tdb-dbt-bigquery-portfolio.dbt_dev.fact_flight_delay_cause_2026`
WHERE flight_date >= DATE '2026-01-01'
  AND flight_date < DATE '2026-03-01'
GROUP BY 2

UNION ALL

SELECT 'delay_mart', CAST(flight_month AS DATE),
       SUM(affected_flights),
       SUM(CAST(total_delay_minutes AS NUMERIC))
FROM `tdb-dbt-bigquery-portfolio.dbt_dev.mart_carrier_monthly_delay_cause_2026`
WHERE CAST(flight_month AS DATE) >= DATE '2026-01-01'
  AND CAST(flight_month AS DATE) < DATE '2026-03-01'
GROUP BY 2
