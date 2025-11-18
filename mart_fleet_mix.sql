CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_marts.mart_fleet_mix` AS
SELECT
  d.year,
  d.month,
  f.date_id,
  CASE
    WHEN ac.aircraft_body_type IN ('WIDE-BODY', 'NARROW-BODY')
      THEN ac.aircraft_body_type
    ELSE 'OTHER'
  END AS body_bucket,
  SUM(f.landing_count) AS landings,
  SUM(f.total_landed_weight) AS landed_weight
FROM `sfo-lakehouse-226-478502.sfo_core.fact_landings_monthly` f
JOIN `sfo-lakehouse-226-478502.sfo_core.dim_date` d
  ON d.date_id = f.date_id
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_aircraft` ac
  ON ac.aircraft_id = f.aircraft_id
GROUP BY d.year, d.month, f.date_id, body_bucket;

