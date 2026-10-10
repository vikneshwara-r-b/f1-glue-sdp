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

-- dim_circuits is enriched with track_type from circuit_track_type, a second,
-- independent, non-API reference source (a streaming table ingesting static
-- flat files from S3 -- see 00_bronze_circuit_track_type.py). This single
-- JOIN reference is what makes SDP add circuit_track_type as an upstream
-- dependency of dim_circuits, with no other orchestration config anywhere.
-- LEFT JOIN (not INNER): a missing/mismatched circuit_Id should surface as
-- track_type = NULL, not silently drop the circuit row (and cascade into
-- dropping it from circuit_summary too). The inner ROW_NUMBER() dedup on
-- circuit_track_type is needed because it's an append-only streaming table --
-- a future corrected/additional row for the same circuit must not fan out
-- this join; latest load_date_time wins, the same SCD1 convention already
-- used for dim_drivers/dim_teams/dim_circuits itself.
CREATE MATERIALIZED VIEW dim_circuits AS
SELECT rc.circuit_Id, rc.name, rc.city, rc.country, ctt.track_type, rc.length, rc.corners,
       rc.first_Participation_Year, rc.lap_Record, rc.fastest_Lap_Driver_Id,
       rc.fastest_Lap_Team_Id, rc.fastest_Lap_Year, rc.race_Date, rc.load_date_time
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY circuit_Id ORDER BY race_Date DESC, load_date_time DESC) AS _rn
  FROM race_circuits
) rc
LEFT JOIN (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY circuit_Id ORDER BY load_date_time DESC) AS _rn
  FROM circuit_track_type
) ctt ON rc.circuit_Id = ctt.circuit_Id AND ctt._rn = 1
WHERE rc._rn = 1;

CREATE MATERIALIZED VIEW fact_race_results AS
SELECT season_year, race_Id, driver_Id, team_Id, circuit_Id, race_Name, race_Round, race_Date, race_Time,
       driver_Race_Grid_Position, driver_Race_Final_Position, driver_Race_Points, driver_Race_Fast_Lap,
       driver_Race_Gap_With_Win_Time, load_date_time AS refreshed_at
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY race_Id, driver_Id ORDER BY load_date_time DESC) AS _rn
  FROM race_results_unnested
) WHERE _rn = 1;
