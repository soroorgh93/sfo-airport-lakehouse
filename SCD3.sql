-- CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.demo_scd3_terminal` AS
-- SELECT
--   terminal_id,
--   terminal,
--   CAST(boarding_area AS STRING) AS current_boarding_area,
--   CAST(NULL AS STRING) AS previous_boarding_area
-- FROM `sfo-lakehouse-226-478502.sfo_core.dim_terminal`;


-- CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.stg_terminal_changes` AS
-- SELECT
--   terminal_id,
--   'G' AS new_boarding_area
-- FROM `sfo-lakehouse-226-478502.sfo_core.demo_scd3_terminal`
-- WHERE terminal = 'INTERNATIONAL';  -- adjust to existing terminal
 
UPDATE `sfo-lakehouse-226-478502.sfo_core.demo_scd3_terminal` t
SET previous_boarding_area = t.current_boarding_area,
    current_boarding_area  = c.new_boarding_area
FROM `sfo-lakehouse-226-478502.sfo_core.stg_terminal_changes` c
WHERE t.terminal_id = c.terminal_id;
