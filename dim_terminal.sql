CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.dim_terminal` AS
SELECT
  GENERATE_UUID() AS terminal_id,
  UPPER(TRIM(Terminal)) AS terminal,
  UPPER(TRIM(`Boarding Area`)) AS boarding_area
FROM (
  SELECT DISTINCT Terminal, `Boarding Area`
  FROM `sfo-lakehouse-226-478502.sfo_raw.passenger_raw`
);
