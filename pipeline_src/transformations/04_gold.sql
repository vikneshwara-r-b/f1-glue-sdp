-- Gold (analytics) layer: same 3 materialized views, joins, aggregations, and grains
-- as the Databricks original. The one necessary mechanical change is that the
-- original's "WHERE __END_AT IS NULL" filters on the dimension CTEs are removed --
-- dim_drivers/dim_teams/dim_circuits are already latest-only under SCD1, so that
-- column/predicate no longer exists. No other business-logic change.

CREATE MATERIALIZED VIEW driver_performance_summary AS
SELECT race_result.season_year, d.driver_Id, d.name AS driver_name, t.team_id AS team_Id, t.name AS team_name,
  COUNT(DISTINCT race_result.race_Id) AS races_entered,
  COUNT(CASE WHEN race_result.driver_Race_Final_Position = 1 THEN d.driver_Id END) AS wins,
  COUNT(CASE WHEN race_result.driver_Race_Final_Position <= 3 THEN d.driver_Id END) AS podiums,
  SUM(race_result.driver_Race_Points) AS total_points,
  AVG(race_result.driver_Race_Final_Position) AS avg_finish_position,
  MIN(race_result.driver_Race_Final_Position) AS best_finish_position,
  current_timestamp() AS refreshed_at
FROM fact_race_results race_result
INNER JOIN dim_drivers d ON race_result.driver_Id = d.driver_Id
INNER JOIN dim_teams t ON race_result.team_Id = t.team_id
GROUP BY race_result.season_year, d.driver_Id, d.name, t.team_id, t.name;

CREATE MATERIALIZED VIEW team_performance_summary AS
SELECT t.team_id AS team_Id, t.name AS team_name,
  COUNT(DISTINCT race_result.race_Id) AS races_entered,
  SUM(CASE WHEN race_result.driver_Race_Final_Position = 1 THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN race_result.driver_Race_Final_Position <= 3 THEN 1 ELSE 0 END) AS podiums,
  SUM(race_result.driver_Race_Points) AS total_points,
  COUNT(DISTINCT race_result.driver_Id) AS drivers_used,
  current_timestamp() AS refreshed_at
FROM fact_race_results race_result
INNER JOIN dim_teams t ON race_result.team_Id = t.team_id
GROUP BY t.team_id, t.name;

CREATE MATERIALIZED VIEW circuit_summary AS
SELECT c.circuit_Id, c.name AS circuit_name, c.city, c.country,
  COUNT(DISTINCT r.race_Id) AS total_races_held,
  MIN(r.race_Date) AS first_race_date,
  MAX(r.race_Date) AS last_race_date,
  c.lap_Record AS lap_record_time,
  c.fastest_Lap_Driver_Id AS fastest_lap_driver_id,
  current_timestamp() AS refreshed_at
FROM dim_circuits c
INNER JOIN fact_race_results r ON c.circuit_Id = r.circuit_Id
GROUP BY c.circuit_Id, c.name, c.city, c.country, c.lap_Record, c.fastest_Lap_Driver_Id;
