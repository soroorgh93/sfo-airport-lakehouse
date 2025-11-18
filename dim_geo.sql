CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.dim_geo` AS
WITH g AS (
  SELECT DISTINCT
    UPPER(TRIM(`GEO Summary`)) AS geo_summary,
    UPPER(TRIM(`GEO Region`)) AS geo_region
  FROM `sfo-lakehouse-226-478502.sfo_raw.passenger_raw`
  UNION DISTINCT
  SELECT DISTINCT
    UPPER(TRIM(`GEO Summary`)),
    UPPER(TRIM(`GEO Region`))
  FROM `sfo-lakehouse-226-478502.sfo_raw.passenger_raw`
)
SELECT
  GENERATE_UUID() AS geo_id,
  geo_summary,
  geo_region
FROM g;
