SELECT 'fact' AS source,
       DATE_TRUNC(flight_date, MONTH) AS flight_month,
       COUNT(*) AS scheduled_flights,
       COUNTIF(is_cancelled) AS cancelled_flights,
       COUNTIF(is_arrival_delayed_15_minutes) AS delayed_arrivals,
       CAST(NULL AS INT64) AS observed_arrivals,
       CAST(NULL AS FLOAT64) AS arrival_minutes,
       CAST(NULL AS FLOAT64) AS departure_minutes
FROM `tdb-dbt-bigquery-portfolio.dbt_dev.fact_flights_2026_monthly_incremental`
WHERE flight_date >= DATE '2026-01-01'
  AND flight_date < DATE '2026-03-01'
GROUP BY 2

UNION ALL

SELECT 'carrier', CAST(flight_month AS DATE),
       SUM(scheduled_flights), SUM(cancelled_flights),
       SUM(arrival_delayed_15_flights),
       SUM(arrival_observed_flights),
       CAST(SUM(total_arrival_delay_minutes) AS FLOAT64),
       CAST(SUM(total_departure_delay_minutes) AS FLOAT64)
FROM `tdb-dbt-bigquery-portfolio.dbt_dev.mart_carrier_monthly_performance_2026`
WHERE CAST(flight_month AS DATE) >= DATE '2026-01-01'
  AND CAST(flight_month AS DATE) < DATE '2026-03-01'
GROUP BY 2

UNION ALL

SELECT 'route', CAST(flight_month AS DATE),
       SUM(scheduled_flights), SUM(cancelled_flights),
       SUM(arrival_delayed_15_flights),
       SUM(arrival_observed_flights),
       CAST(SUM(total_arrival_delay_minutes) AS FLOAT64),
       CAST(SUM(total_departure_delay_minutes) AS FLOAT64)
FROM `tdb-dbt-bigquery-portfolio.dbt_dev.mart_route_monthly_performance_2026`
WHERE CAST(flight_month AS DATE) >= DATE '2026-01-01'
  AND CAST(flight_month AS DATE) < DATE '2026-03-01'
GROUP BY 2

UNION ALL

SELECT 'airport_departure', CAST(flight_month AS DATE),
       SUM(scheduled_departures), SUM(cancelled_departures),
       CAST(NULL AS INT64), CAST(NULL AS INT64),
       CAST(NULL AS FLOAT64),
       CAST(SUM(total_departure_delay_minutes) AS FLOAT64)
FROM `tdb-dbt-bigquery-portfolio.dbt_dev.mart_airport_monthly_performance_2026`
WHERE CAST(flight_month AS DATE) >= DATE '2026-01-01'
  AND CAST(flight_month AS DATE) < DATE '2026-03-01'
GROUP BY 2

UNION ALL

SELECT 'airport_arrival', CAST(flight_month AS DATE),
       SUM(scheduled_arrivals), CAST(NULL AS INT64),
       SUM(arrival_delayed_15_flights),
       SUM(arrival_observed_flights),
       CAST(SUM(total_arrival_delay_minutes) AS FLOAT64),
       CAST(NULL AS FLOAT64)
FROM `tdb-dbt-bigquery-portfolio.dbt_dev.mart_airport_monthly_performance_2026`
WHERE CAST(flight_month AS DATE) >= DATE '2026-01-01'
  AND CAST(flight_month AS DATE) < DATE '2026-03-01'
GROUP BY 2
