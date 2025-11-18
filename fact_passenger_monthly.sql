CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.fact_passenger_monthly` AS
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
    UPPER(TRIM(Terminal)) AS terminal,
    UPPER(TRIM(`Boarding Area`)) AS boarding_area,
    UPPER(TRIM(`Activity Type Code`)) AS activity_type_code,
    UPPER(TRIM(`Price Category Code`)) AS price_category_code,
    SUM(CAST(`Passenger Count` AS INT64)) AS passenger_count
  FROM `sfo-lakehouse-226-478502.sfo_raw.passenger_raw`
  GROUP BY
    activity_period, month_start,
    operating_iata, operating_airline,
    published_iata, published_airline,
    geo_summary, geo_region,
    terminal, boarding_area,
    activity_type_code,
    price_category_code
),
with_keys AS (
  SELECT
    activity_period,
    month_start,
    IFNULL(NULLIF(operating_iata,''), UPPER(REGEXP_REPLACE(operating_airline, r'[^A-Z0-9]', ''))) AS operating_airline_key,
    IFNULL(NULLIF(published_iata,''), UPPER(REGEXP_REPLACE(published_airline, r'[^A-Z0-9]', ''))) AS published_airline_key,
    geo_summary,
    geo_region,
    terminal,
    boarding_area,
    activity_type_code,
    price_category_code,
    passenger_count
  FROM base
)
SELECT
  d.date_id,
  ao.airline_id AS operating_airline_id,
  ap.airline_id AS published_airline_id,
  g.geo_id,
  t.terminal_id,
  act.activity_id,
  pc.price_category_id,
  passenger_count
FROM with_keys bk
JOIN `sfo-lakehouse-226-478502.sfo_core.dim_date` d
  ON d.date_id = bk.activity_period
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_airline` ao
  ON ao.airline_key = bk.operating_airline_key
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_airline` ap
  ON ap.airline_key = bk.published_airline_key
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_geo` g
  ON g.geo_summary = bk.geo_summary AND g.geo_region = bk.geo_region
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_terminal` t
  ON t.terminal = bk.terminal AND t.boarding_area = bk.boarding_area
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_activity` act
  ON act.activity_type_code = bk.activity_type_code
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_price_category` pc
  ON pc.price_category_code = bk.price_category_code;
