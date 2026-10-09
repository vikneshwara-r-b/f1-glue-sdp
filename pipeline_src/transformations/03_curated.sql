-- Curated (silver) layer: SCD Type 1 (latest-state-only, no history) for all four
-- tables. AWS Glue 6.0 SDP has no create_auto_cdc_flow/APPLY CHANGES primitive (the
-- Databricks original used it for SCD2 dims + SCD1 fact), so "latest row per key" is
-- computed here via ROW_NUMBER() over the full bronze-prepared history, materialized
-- as a view (full recompute each run -- the only way to resolve "current state" without
-- a native upsert/merge operator). Sequence columns match the Databricks original's
-- sequence_by: race_Date for the three dimensions, load_date_time ("refreshed_at")
-- for the fact. A secondary sort on load_date_time breaks ties deterministically.

CREATE MATERIALIZED VIEW dim_drivers AS
SELECT driver_Id, name, surname, shortName, nationality, birthday, number, race_Date, load_date_time
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY driver_Id ORDER BY race_Date DESC, load_date_time DESC) AS _rn
  FROM race_drivers
) WHERE _rn = 1;

CREATE MATERIALIZED VIEW dim_teams AS
SELECT team_id, name, nationality, first_Appearance, constructors_championships, drivers_championships, race_Date, load_date_time
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY race_Date DESC, load_date_time DESC) AS _rn
  FROM race_teams
) WHERE _rn = 1;

CREATE MATERIALIZED VIEW dim_circuits AS
SELECT circuit_Id, name, city, country, length, corners, first_Participation_Year, lap_Record,
       fastest_Lap_Driver_Id, fastest_Lap_Team_Id, fastest_Lap_Year, race_Date, load_date_time
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY circuit_Id ORDER BY race_Date DESC, load_date_time DESC) AS _rn
  FROM race_circuits
) WHERE _rn = 1;

CREATE MATERIALIZED VIEW fact_race_results AS
SELECT season_year, race_Id, driver_Id, team_Id, circuit_Id, race_Name, race_Round, race_Date, race_Time,
       driver_Race_Grid_Position, driver_Race_Final_Position, driver_Race_Points, driver_Race_Fast_Lap,
       driver_Race_Gap_With_Win_Time, load_date_time AS refreshed_at
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY race_Id, driver_Id ORDER BY load_date_time DESC) AS _rn
  FROM race_results_unnested
) WHERE _rn = 1;
