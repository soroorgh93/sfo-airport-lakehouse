CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.dim_activity` AS
SELECT
  GENERATE_UUID() AS activity_id,
  UPPER(TRIM(`Activity Type Code`)) AS activity_type_code
FROM (
  SELECT DISTINCT `Activity Type Code`
  FROM `sfo-lakehouse-226-478502.sfo_raw.passenger_raw`
);

