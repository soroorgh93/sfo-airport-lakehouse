CREATE OR REPLACE TABLE `sfo-lakehouse-226-478502.sfo_marts.mart_terminal_load_monthly` AS
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
FROM `sfo-lakehouse-226-478502.sfo_core.fact_passenger_monthly` f
JOIN `sfo-lakehouse-226-478502.sfo_core.dim_date` d
  ON d.date_id = f.date_id
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_terminal` t
  ON t.terminal_id = f.terminal_id
LEFT JOIN `sfo-lakehouse-226-478502.sfo_core.dim_activity` act
  ON act.activity_id = f.activity_id
GROUP BY
  f.date_id, d.year, d.month, t.terminal, t.boarding_area;
