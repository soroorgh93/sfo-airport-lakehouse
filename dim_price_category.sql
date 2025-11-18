CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.dim_price_category` AS
SELECT
  GENERATE_UUID() AS price_category_id,
  UPPER(TRIM(`Price Category Code`)) AS price_category_code
FROM (
  SELECT DISTINCT `Price Category Code`
  FROM `sfo-lakehouse-226-478502.sfo_raw.passenger_raw`
);
