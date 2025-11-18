CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.fact_landings_monthly` AS
WITH base AS (
  SELECT
    CAST(`Activity Period` AS INT64) AS activity_period,
    PARSE_DATE('%Y/%m/%d', `Activity Period Start Date`) AS month_start,
    UPPER(TRIM(`Operating Airline IATA Code`)) AS operating_iata,
    UPPER(TRIM(`Operating Airline`)) AS operating_airline,
    UPPER(TRIM(`Published Airline IATA Code`)) AS published_iata,
    UPPER(TRIM(`Published Airline`)) AS published_airline,
    UPPER(TRIM(`GEO Summary`)) AS geo_summary,
    UPPER(TRIM(`GEO Region`)) AS geo_region,
    UPPER(TRIM(`Landing Aircraft Type`)) AS landing_aircraft_type,
    UPPER(TRIM(`Aircraft Body Type`)) AS aircraft_body_type,
    UPPER(TRIM(`Aircraft Manufacturer`)) AS aircraft_manufacturer,
    UPPER(TRIM(`Aircraft Model`)) AS aircraft_model,
    UPPER(TRIM(`Aircraft Version`)) AS aircraft_version,
    SUM(CAST(`Landing Count` AS INT64)) AS landing_count,
    SUM(CAST(`Total Landed Weight` AS INT64)) AS total_landed_weight
  FROM `sfo-lakehouse-226-478502.sfo_raw.landings_raw`
  GROUP BY
    activity_period, month_start,
    operating_iata, operating_airline,
    published_iata, published_airline,
    geo_summary, geo_region,
    landing_aircraft_type,
    aircraft_body_type,
    aircraft_manufacturer,
    aircraft_model,
    aircraft_version
),
with_keys AS (
  SELECT
    activity_period,
    month_start,
    IFNULL(NULLIF(operating_iata,''), UPPER(REGEXP_REPLACE(operating_airline, r'[^A-Z0-9]', ''))) AS operating_airline_key,
    IFNULL(NULLIF(published_iata,''), UPPER(REGEXP_REPLACE(published_airline, r'[^A-Z0-9]', ''))) AS published_airline_key,
    geo_summary,
    geo_region,
    landing_aircraft_type,
    aircraft_body_type,
    aircraft_manufacturer,
    aircraft_model,
    aircraft_version,
    landing_count,
    total_landed_weight
  FROM base
)
SELECT
  d.date_id,
  ao.airline_id AS operating_airline_id,
  ap.airline_id AS published_airline_id,
  g.geo_id,
  ac.aircraft_id,
  landing_count,
  total_landed_weight
FROM with_keys bk
JOIN `sfo-lakehouse-226-478502.sfo_core.dim_date` d
  ON d.date_id = bk.activity_period
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_airline` ao
  ON ao.airline_key = bk.operating_airline_key
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_airline` ap
  ON ap.airline_key = bk.published_airline_key
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_geo` g
  ON g.geo_summary = bk.geo_summary AND g.geo_region = bk.geo_region
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_aircraft` ac
  ON ac.landing_aircraft_type = bk.landing_aircraft_type
 AND ac.aircraft_body_type = bk.aircraft_body_type
 AND ac.aircraft_manufacturer = bk.aircraft_manufacturer
 AND ac.aircraft_model = bk.aircraft_model
 AND ac.aircraft_version = bk.aircraft_version;

