CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.dim_aircraft` AS
SELECT
  GENERATE_UUID() AS aircraft_id,
  UPPER(TRIM(`Landing Aircraft Type`)) AS landing_aircraft_type,
  UPPER(TRIM(`Aircraft Body Type`)) AS aircraft_body_type,
  UPPER(TRIM(`Aircraft Manufacturer`)) AS aircraft_manufacturer,
  UPPER(TRIM(`Aircraft Model`)) AS aircraft_model,
  UPPER(TRIM(`Aircraft Version`)) AS aircraft_version
FROM (
  SELECT DISTINCT
    `Landing Aircraft Type`,
    `Aircraft Body Type`,
    `Aircraft Manufacturer`,
    `Aircraft Model`,
    `Aircraft Version`
  FROM `sfo-lakehouse-226-478502.sfo_raw.landings_raw`
);
