-- CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.demo_scd1_airline` AS
-- SELECT * FROM `sfo-lakehouse-226-478502.sfo_core.dim_airline`;

-- CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_core.stg_airline_corrections` AS
-- SELECT 'UA' AS airline_key, 'UNITED AIRLINES, INC.' AS new_name UNION ALL
-- SELECT 'AA', 'AMERICAN AIRLINES, INC.' AS new_name;

MERGE `sfo-lakehouse-226-478502.sfo_core.demo_scd1_airline` AS tgt
USING `sfo-lakehouse-226-478502.sfo_core.stg_airline_corrections` AS src
ON tgt.airline_key = src.airline_key
WHEN MATCHED THEN
  UPDATE SET airline_name = src.new_name;
