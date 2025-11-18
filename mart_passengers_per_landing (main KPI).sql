CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_marts.mart_passengers_per_landing` AS
WITH passengers AS (
  SELECT
    f.date_id,
    f.operating_airline_id,
    f.geo_id,
    SUM(
      CASE WHEN act.activity_type_code = 'THRU / TRANSIT'
           THEN 0
           ELSE f.passenger_count
      END
    ) AS passengers_excl_transit
  FROM `sfo-lakehouse-226-478502.sfo_core.fact_passenger_monthly` f
  JOIN `sfo-lakehouse-226-478502.sfo_core.dim_activity` act
    ON act.activity_id = f.activity_id
  GROUP BY f.date_id, f.operating_airline_id, f.geo_id
),
landings AS (
  SELECT
    date_id,
    operating_airline_id,
    geo_id,
    SUM(landing_count) AS landings
  FROM `sfo-lakehouse-226-478502.sfo_core.fact_landings_monthly`
  GROUP BY date_id, operating_airline_id, geo_id
)
SELECT
  d.year,
  d.month,
  p.date_id,
  ao.airline_name AS operating_airline,
  g.geo_region,
  p.passengers_excl_transit,
  l.landings,
  SAFE_DIVIDE(p.passengers_excl_transit, l.landings) AS passengers_per_landing
FROM passengers p
JOIN landings l
  ON p.date_id = l.date_id
 AND p.operating_airline_id = l.operating_airline_id
 AND p.geo_id = l.geo_id
JOIN `sfo-lakehouse-226-478502.sfo_core.dim_date` d
  ON d.date_id = p.date_id
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_airline` ao
  ON ao.airline_id = p.operating_airline_id
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_geo` g
  ON g.geo_id = p.geo_id;

