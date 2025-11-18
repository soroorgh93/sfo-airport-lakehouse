CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.dim_airline` AS
WITH airlines AS (
  SELECT DISTINCT
    UPPER(TRIM(`Operating Airline IATA Code`)) AS iata_code,
    UPPER(TRIM(`Operating Airline`)) AS airline_name
  FROM `sfo-lakehouse-226-478502.sfo_raw.passenger_raw`
  UNION DISTINCT
  SELECT DISTINCT
    UPPER(TRIM(`Published Airline IATA Code`)),
    UPPER(TRIM(`Published Airline`))
  FROM `sfo-lakehouse-226-478502.sfo_raw.passenger_raw`
  UNION DISTINCT
  SELECT DISTINCT
    UPPER(TRIM(`Operating Airline IATA Code`)),
    UPPER(TRIM(`Operating Airline`))
  FROM `sfo-lakehouse-226-478502.sfo_raw.landings_raw`
  UNION DISTINCT
  SELECT DISTINCT
    UPPER(TRIM(`Published Airline IATA Code`)),
    UPPER(TRIM(`Published Airline`))
  FROM `sfo-lakehouse-226-478502.sfo_raw.landings_raw`
),
cleaned AS (
  SELECT
    IFNULL(NULLIF(iata_code, ''), UPPER(REGEXP_REPLACE(airline_name, r'[^A-Z0-9]', ''))) AS airline_key,
    airline_name
  FROM airlines
)
SELECT
  GENERATE_UUID() AS airline_id,   -- surrogate key
  airline_key,                     -- business key
  airline_name
FROM cleaned;
