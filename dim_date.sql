CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.dim_date` AS
WITH months AS (
  SELECT
    CAST(`Activity Period` AS INT64) AS activity_period,
    PARSE_DATE('%Y/%m/%d', `Activity Period Start Date`) AS month_start
  FROM `sfo-lakehouse-226-478502.sfo_raw.passenger_raw`
  UNION DISTINCT
  SELECT
    CAST(`Activity Period` AS INT64),
    PARSE_DATE('%Y/%m/%d', `Activity Period Start Date`)
  FROM `sfo-lakehouse-226-478502.sfo_raw.landings_raw`
)

SELECT
  activity_period AS date_id,
  month_start,
  EXTRACT(YEAR FROM month_start) AS year,
  EXTRACT(MONTH FROM month_start) AS month,
  CONCAT('Q', CAST(EXTRACT(QUARTER FROM month_start) AS STRING)) AS quarter
FROM months;
