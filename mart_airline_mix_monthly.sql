CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_marts.mart_airline_mix_monthly` AS
SELECT
  f.date_id,
  d.year,
  d.month,
  ao.airline_name AS operating_airline,
  ao.airline_key  AS operating_airline_code,
  g.geo_summary,
  g.geo_region,
  SUM(
    CASE WHEN act.activity_type_code = 'THRU / TRANSIT'
         THEN 0
         ELSE f.passenger_count
    END
  ) AS passengers_excl_transit,
  SUM(f.passenger_count) AS passengers_incl_transit
FROM `sfo-lakehouse-226-478502.sfo_core.fact_passenger_monthly` f
JOIN `sfo-lakehouse-226-478502.sfo_core.dim_date` d
  ON d.date_id = f.date_id
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_airline` ao
  ON ao.airline_id = f.operating_airline_id
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_geo` g
  ON g.geo_id = f.geo_id
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_activity` act
  ON act.activity_id = f.activity_id
GROUP BY
  f.date_id, d.year, d.month,
  operating_airline, operating_airline_code,
  g.geo_summary, g.geo_region;
