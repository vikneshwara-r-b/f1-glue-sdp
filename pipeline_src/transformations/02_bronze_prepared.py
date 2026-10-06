from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Ported 1:1 from the Databricks original's business logic
# (race_data_load_raw_and_prepared.py). Every derived column is expressed via
# .select(...), never .withColumn(...), per AWS's documented SDP guidance: a
# downstream dataset reading an upstream pipeline table via spark.table(...)/
# .table(...) and then applying .withColumn(...) can cause SDP to miss the
# dependency on the second and subsequent runs (one-run-lag stale reads).
#
# These helpers live here (not in a separate f1_pipeline_lib module) because
# each is only ever called from exactly one flow below -- no reuse across
# transformation files to justify a shared library, and they can't be unit
# tested standalone anyway: dp.create_streaming_table()/@dp.append_flow require
# an active SDP pipeline-graph context, which only exists inside a real
# pipeline build (confirmed: calling them at plain module import raises
# GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE).


def _cast_raw_race(raw_race: DataFrame) -> DataFrame:
    """Casts/aliases the raw API payload's top-level fields and keeps the nested
    circuit/results arrays as-is for downstream explode steps."""
    return raw_race.select(
        F.col("url").alias("api_url"),
        F.col("season").alias("season_year"),
        F.col("races.round").cast("integer").alias("race_Round"),
        F.col("races.date").cast("date").alias("race_Date"),
        F.col("races.time").alias("race_Time"),
        F.col("races.raceId").alias("race_Id"),
        F.col("races.raceName").alias("race_Name"),
        F.col("races.circuit").alias("circuit_nested"),
        F.col("races.results").alias("results_nested"),
        "load_date_time",
    )


def _exploded(race_results: DataFrame) -> DataFrame:
    return race_results.select("*", F.explode(F.col("results_nested")).alias("value"))


def _valid_race_results() -> DataFrame:
    return _cast_raw_race(SparkSession.active().readStream.table("raw_race"))


dp.create_streaming_table(
    "race_results_unnested",
    comment="One row per (race, driver).",
    schema=(
        "season_year integer,race_Id string,driver_Id string,team_Id string,circuit_Id string,"
        "race_Name string,race_Round integer,race_Date date,race_Time string,"
        "driver_Race_Grid_Position string,driver_Race_Final_Position string,"
        "driver_Race_Points float,driver_Race_Fast_Lap string,driver_Race_Gap_With_Win_Time string,"
        "load_date_time timestamp"
    ),
)
dp.create_streaming_table(
    "race_drivers",
    comment="One row per (race, driver) occurrence, not deduped.",
    schema=(
        "driver_Id string,name string,surname string,shortName string,nationality string,"
        "birthday date,number integer,race_Date date,load_date_time timestamp"
    ),
)
dp.create_streaming_table(
    "race_teams",
    comment="Deduped team attributes.",
    schema=(
        "team_id string,name string,nationality string,first_Appearance integer,"
        "constructors_championships integer,drivers_championships integer,"
        "race_Date date,load_date_time timestamp"
    ),
)
dp.create_streaming_table(
    "race_circuits",
    comment="One row per race (circuit is 1-per-race).",
    schema=(
        "circuit_Id string,name string,city string,country string,length string,"
        "corners integer,first_Participation_Year integer,lap_Record string,"
        "fastest_Lap_Driver_Id string,fastest_Lap_Team_Id string,fastest_Lap_Year integer,"
        "race_Date date,load_date_time timestamp"
    ),
)


# No separate "race_results_valid" table: there's no quarantine/validity filter to
# gate on in v1 (deferred), so each flow below reads raw_race directly and applies
# the cast/alias step (_cast_raw_race) inline before its own flatten step.


@dp.append_flow(target="race_results_unnested")
def ingest_race_results_unnested():
    return _exploded(_valid_race_results()).select(
        F.col("season_year").cast("integer"),
        "race_Id",
        F.col("value.driver.driverId").alias("driver_Id"),
        F.col("value.team.teamId").alias("team_Id"),
        F.expr("circuit_nested[0].circuitId").alias("circuit_Id"),
        "race_Name",
        "race_Round",
        "race_Date",
        "race_Time",
        F.col("value.grid").alias("driver_Race_Grid_Position"),
        F.col("value.position").alias("driver_Race_Final_Position"),
        F.col("value.points").cast("float").alias("driver_Race_Points"),
        F.col("value.fastLap").alias("driver_Race_Fast_Lap"),
        F.col("value.time").alias("driver_Race_Gap_With_Win_Time"),
        "load_date_time",
    )


@dp.append_flow(target="race_drivers")
def ingest_race_drivers():
    # One row per (race, driver) occurrence -- not deduped (same as the
    # Databricks original); dim_drivers resolves the latest version downstream.
    #
    # birthday needs two date formats, not a plain .cast("date"): confirmed on a
    # real Glue RUN that the F1 API returns driver birthdays in a mix of
    # yyyy-MM-dd (e.g. max_verstappen "1997-09-30") and dd/MM/yyyy (e.g. sainz
    # "01/09/1994", verified against his real birthdate of 1 Sep 1994 -- day
    # comes first) -- a bare cast("date") only handles the first form and raised
    # [CAST_INVALID_INPUT] on the second. Must use try_to_date, not to_date:
    # Spark 4.x has ANSI mode on by default, under which to_date() *raises* on a
    # non-matching format rather than returning null (confirmed locally) --
    # try_to_date returns null instead, which coalesce then falls through on.
    return _exploded(_valid_race_results()).select(
        F.col("value.driver.driverId").alias("driver_Id"),
        F.col("value.driver.name").alias("name"),
        F.col("value.driver.surname").alias("surname"),
        F.col("value.driver.shortName").alias("shortName"),
        F.col("value.driver.nationality").alias("nationality"),
        F.coalesce(
            F.try_to_date(F.col("value.driver.birthday"), "yyyy-MM-dd"),
            F.try_to_date(F.col("value.driver.birthday"), "dd/MM/yyyy"),
        ).alias("birthday"),
        F.col("value.driver.number").cast("integer").alias("number"),
        "race_Date",
        "load_date_time",
    )


@dp.append_flow(target="race_teams")
def ingest_race_teams():
    # NOTE: preserves the Databricks original's lowercase "team_id" alias (vs.
    # "team_Id" elsewhere) to keep the same business logic. Spark SQL's default
    # case-insensitive identifier resolution means joins against "team_Id"
    # elsewhere still work.
    #
    # No .distinct() here (removed -- confirmed on a real Glue run to break
    # flow registration): .distinct() on a streaming DataFrame compiles to a
    # Deduplicate relation node, which forces the server-side analyzer to fully
    # resolve the upstream plan -- including raw_race -- at flow-registration
    # time. The simple projection-only flows elsewhere in this file stay
    # unresolved/lazy and never hit that wall, but this one raised
    # [TABLE_OR_VIEW_NOT_FOUND] for raw_race because of it. Not a loss: dedup is
    # redundant here anyway -- dim_teams in 03_curated.sql already resolves one
    # row per team_id via ROW_NUMBER(), so race_teams carrying one row per
    # occurrence (like race_drivers/race_circuits) is fine.
    return _exploded(_valid_race_results()).select(
        F.col("value.team.teamId").alias("team_id"),
        F.col("value.team.teamName").alias("name"),
        F.col("value.team.nationality").alias("nationality"),
        F.col("value.team.firstAppareance").cast("integer").alias("first_Appearance"),
        F.col("value.team.constructorsChampionships").cast("integer").alias(
            "constructors_championships"
        ),
        F.col("value.team.driversChampionships").cast("integer").alias(
            "drivers_championships"
        ),
        "race_Date",
        "load_date_time",
    )


@dp.append_flow(target="race_circuits")
def ingest_race_circuits():
    # One row per race (circuit is 1-per-race via circuit_nested[0]).
    return _valid_race_results().select(
        F.expr("circuit_nested[0].circuitId").alias("circuit_Id"),
        F.expr("circuit_nested[0].circuitName").alias("name"),
        F.expr("circuit_nested[0].city").alias("city"),
        F.expr("circuit_nested[0].country").alias("country"),
        F.expr("circuit_nested[0].circuitLength").alias("length"),
        F.expr("circuit_nested[0].corners").cast("integer").alias("corners"),
        F.expr("circuit_nested[0].firstParticipationYear").cast("integer").alias(
            "first_Participation_Year"
        ),
        F.expr("circuit_nested[0].lapRecord").alias("lap_Record"),
        F.expr("circuit_nested[0].fastestLapDriverId").alias("fastest_Lap_Driver_Id"),
        F.expr("circuit_nested[0].fastestLapTeamId").alias("fastest_Lap_Team_Id"),
        F.expr("circuit_nested[0].fastestLapYear").cast("integer").alias("fastest_Lap_Year"),
        "race_Date",
        "load_date_time",
    )
