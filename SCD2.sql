-- CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.demo_scd2_geo` AS
-- SELECT
--   GENERATE_UUID() AS geo_sk,
--   geo_id,
--   geo_summary,
--   geo_region,
--   DATE '1900-01-01' AS effective_start_date,
--   DATE '9999-12-31' AS effective_end_date,
--   TRUE AS is_current
-- FROM `sfo-lakehouse-226-478502.sfo_core.dim_geo`;

-- CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.stg_geo_changes` AS
-- SELECT
--   geo_id,
--   geo_summary,
--   'EMEA (EUROPE+MIDDLE EAST)' AS new_region
-- FROM `sfo-lakehouse-226-478502.sfo_core.dim_geo`
-- WHERE geo_region = 'EUROPE';

-- UPDATE `sfo-lakehouse-226-478502.sfo_core.demo_scd2_geo` tgt
-- SET effective_end_date = CURRENT_DATE(),
--     is_current = FALSE
-- WHERE tgt.geo_id IN (
--   SELECT geo_id FROM `sfo-lakehouse-226-478502.sfo_core.stg_geo_changes`
-- )
-- AND tgt.is_current = TRUE;

INSERT INTO `sfo-lakehouse-226-478502.sfo_core.demo_scd2_geo`
(geo_sk, geo_id, geo_summary, geo_region, effective_start_date, effective_end_date, is_current)
SELECT
  GENERATE_UUID(),
  geo_id,
  geo_summary,
  new_region,
  CURRENT_DATE(),
  DATE '9999-12-31',
  TRUE
FROM `sfo-lakehouse-226-478502.sfo_core.stg_geo_changes`;

