--dim_date
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_core.dim_date` AS
WITH months AS (
  SELECT
    CAST(`Activity Period` AS INT64) AS activity_period,
    PARSE_DATE('%Y/%m/%d', `Activity Period Start Date`) AS month_start
  FROM `sfo-lakehouse-226.sfo_raw.passenger_raw`
  UNION DISTINCT
  SELECT
    CAST(`Activity Period` AS INT64),
    PARSE_DATE('%Y/%m/%d', `Activity Period Start Date`)
  FROM `sfo-lakehouse-226.sfo_raw.landings_raw`
)

SELECT
  activity_period AS date_id,
  month_start,
  EXTRACT(YEAR FROM month_start) AS year,
  EXTRACT(MONTH FROM month_start) AS month,
  CONCAT('Q', CAST(EXTRACT(QUARTER FROM month_start) AS STRING)) AS quarter
FROM months;
--dim_airline (with surrogate key)
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_core.dim_airline` AS
WITH airlines AS (
  SELECT DISTINCT
    UPPER(TRIM(`Operating Airline IATA Code`)) AS iata_code,
    UPPER(TRIM(`Operating Airline`)) AS airline_name
  FROM `sfo-lakehouse-226.sfo_raw.passenger_raw`
  UNION DISTINCT
  SELECT DISTINCT
    UPPER(TRIM(`Published Airline IATA Code`)),
    UPPER(TRIM(`Published Airline`))
  FROM `sfo-lakehouse-226.sfo_raw.passenger_raw`
  UNION DISTINCT
  SELECT DISTINCT
    UPPER(TRIM(`Operating Airline IATA Code`)),
    UPPER(TRIM(`Operating Airline`))
  FROM `sfo-lakehouse-226.sfo_raw.landings_raw`
  UNION DISTINCT
  SELECT DISTINCT
    UPPER(TRIM(`Published Airline IATA Code`)),
    UPPER(TRIM(`Published Airline`))
  FROM `sfo-lakehouse-226.sfo_raw.landings_raw`
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

--dim_geo
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_core.dim_geo` AS
WITH g AS (
  SELECT DISTINCT
    UPPER(TRIM(`GEO Summary`)) AS geo_summary,
    UPPER(TRIM(`GEO Region`)) AS geo_region
  FROM `sfo-lakehouse-226.sfo_raw.passenger_raw`
  UNION DISTINCT
  SELECT DISTINCT
    UPPER(TRIM(`GEO Summary`)),
    UPPER(TRIM(`GEO Region`))
  FROM `sfo-lakehouse-226.sfo_raw.landings_raw`
)
SELECT
  GENERATE_UUID() AS geo_id,
  geo_summary,
  geo_region
FROM g;

--dim_terminal
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_core.dim_terminal` AS
SELECT
  GENERATE_UUID() AS terminal_id,
  UPPER(TRIM(Terminal)) AS terminal,
  UPPER(TRIM(`Boarding Area`)) AS boarding_area
FROM (
  SELECT DISTINCT Terminal, `Boarding Area`
  FROM `sfo-lakehouse-226.sfo_raw.passenger_raw`
);

--dim_activity
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_core.dim_activity` AS
SELECT
  GENERATE_UUID() AS activity_id,
  UPPER(TRIM(`Activity Type Code`)) AS activity_type_code
FROM (
  SELECT DISTINCT `Activity Type Code`
  FROM `sfo-lakehouse-226.sfo_raw.passenger_raw`
);

--dim_price_category
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_core.dim_price_category` AS
SELECT
  GENERATE_UUID() AS price_category_id,
  UPPER(TRIM(`Price Category Code`)) AS price_category_code
FROM (
  SELECT DISTINCT `Price Category Code`
  FROM `sfo-lakehouse-226.sfo_raw.passenger_raw`
);

--dim_aircraft
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_core.dim_aircraft` AS
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
  FROM `sfo-lakehouse-226.sfo_raw.landings_raw`
);

--fact_passenger_monthly
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_core.fact_passenger_monthly` AS
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
  FROM `sfo-lakehouse-226.sfo_raw.passenger_raw`
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
JOIN `sfo-lakehouse-226.sfo_core.dim_date` d
  ON d.date_id = bk.activity_period
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_airline` ao
  ON ao.airline_key = bk.operating_airline_key
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_airline` ap
  ON ap.airline_key = bk.published_airline_key
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_geo` g
  ON g.geo_summary = bk.geo_summary AND g.geo_region = bk.geo_region
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_terminal` t
  ON t.terminal = bk.terminal AND t.boarding_area = bk.boarding_area
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_activity` act
  ON act.activity_type_code = bk.activity_type_code
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_price_category` pc
  ON pc.price_category_code = bk.price_category_code;

--fact_landings_monthly
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_core.fact_landings_monthly` AS
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
  FROM `sfo-lakehouse-226.sfo_raw.landings_raw`
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
JOIN `sfo-lakehouse-226.sfo_core.dim_date` d
  ON d.date_id = bk.activity_period
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_airline` ao
  ON ao.airline_key = bk.operating_airline_key
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_airline` ap
  ON ap.airline_key = bk.published_airline_key
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_geo` g
  ON g.geo_summary = bk.geo_summary AND g.geo_region = bk.geo_region
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_aircraft` ac
  ON ac.landing_aircraft_type = bk.landing_aircraft_type
 AND ac.aircraft_body_type = bk.aircraft_body_type
 AND ac.aircraft_manufacturer = bk.aircraft_manufacturer
 AND ac.aircraft_model = bk.aircraft_model
 AND ac.aircraft_version = bk.aircraft_version;



--Marts & KPI SQL 
--mart_airline_mix_monthly
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_marts.mart_airline_mix_monthly` AS
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
FROM `sfo-lakehouse-226.sfo_core.fact_passenger_monthly` f
JOIN `sfo-lakehouse-226.sfo_core.dim_date` d
  ON d.date_id = f.date_id
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_airline` ao
  ON ao.airline_id = f.operating_airline_id
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_geo` g
  ON g.geo_id = f.geo_id
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_activity` act
  ON act.activity_id = f.activity_id
GROUP BY
  f.date_id, d.year, d.month,
  operating_airline, operating_airline_code,
  g.geo_summary, g.geo_region;

--mart_terminal_load_monthly
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_marts.mart_terminal_load_monthly` AS
SELECT
  f.date_id,
  d.year,
  d.month,
  t.terminal,
  t.boarding_area,
  SUM(
    CASE WHEN act.activity_type_code = 'THRU / TRANSIT'
         THEN 0
         ELSE f.passenger_count
    END
  ) AS pax_excl_transit,
  SUM(f.passenger_count) AS pax_incl_transit
FROM `sfo-lakehouse-226.sfo_core.fact_passenger_monthly` f
JOIN `sfo-lakehouse-226.sfo_core.dim_date` d
  ON d.date_id = f.date_id
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_terminal` t
  ON t.terminal_id = f.terminal_id
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_activity` act
  ON act.activity_id = f.activity_id
GROUP BY
  f.date_id, d.year, d.month, t.terminal, t.boarding_area;

--mart_passengers_per_landing (main KPI)
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_marts.mart_passengers_per_landing` AS
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
  FROM `sfo-lakehouse-226.sfo_core.fact_passenger_monthly` f
  JOIN `sfo-lakehouse-226.sfo_core.dim_activity` act
    ON act.activity_id = f.activity_id
  GROUP BY f.date_id, f.operating_airline_id, f.geo_id
),
landings AS (
  SELECT
    date_id,
    operating_airline_id,
    geo_id,
    SUM(landing_count) AS landings
  FROM `sfo-lakehouse-226.sfo_core.fact_landings_monthly`
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
JOIN `sfo-lakehouse-226.sfo_core.dim_date` d
  ON d.date_id = p.date_id
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_airline` ao
  ON ao.airline_id = p.operating_airline_id
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_geo` g
  ON g.geo_id = p.geo_id;

--mart_fleet_mix
CREATE OR REPLACE TABLE `sfo-lakehouse-226.sfo_marts.mart_fleet_mix` AS
SELECT
  d.year,
  d.month,
  f.date_id,
  CASE
    WHEN ac.aircraft_body_type IN ('WIDE-BODY', 'NARROW-BODY')
      THEN ac.aircraft_body_type
    ELSE 'OTHER'
  END AS body_bucket,
  SUM(f.landing_count) AS landings,
  SUM(f.total_landed_weight) AS landed_weight
FROM `sfo-lakehouse-226.sfo_core.fact_landings_monthly` f
JOIN `sfo-lakehouse-226.sfo_core.dim_date` d
  ON d.date_id = f.date_id
LEFT JOIN `sfo-lakehouse-226.sfo_core.dim_aircraft` ac
  ON ac.aircraft_id = f.aircraft_id
GROUP BY d.year, d.month, f.date_id, body_bucket;

