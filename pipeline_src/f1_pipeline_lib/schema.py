from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

# Ported from the Databricks original's bronze layer (race_data_load_raw_and_prepared.py),
# which built this from f1api.dev's `{base_url}/{season}/{round}/race` response shape.
#
# Built via explicit StructType/StructField construction rather than
# StructType.fromDDL(...): fromDDL requires an active SparkContext at call time
# (it parses the DDL string through the JVM), which makes it fail at plain module
# import if nothing has created a SparkSession yet. Building the schema directly
# as Python objects has no such dependency, so this module is safe to import
# standalone (e.g. in tests, or if the SDP runtime imports transformation files
# before/without an active session).

_CIRCUIT_SCHEMA = StructType(
    [
        StructField("circuitId", StringType(), True),
        StructField("circuitLength", StringType(), True),
        StructField("circuitName", StringType(), True),
        StructField("city", StringType(), True),
        StructField("corners", LongType(), True),
        StructField("country", StringType(), True),
        StructField("fastestLapDriverId", StringType(), True),
        StructField("fastestLapTeamId", StringType(), True),
        StructField("fastestLapYear", LongType(), True),
        StructField("firstParticipationYear", LongType(), True),
        StructField("lapRecord", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

_DRIVER_SCHEMA = StructType(
    [
        StructField("birthday", StringType(), True),
        StructField("driverId", StringType(), True),
        StructField("name", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("number", LongType(), True),
        StructField("shortName", StringType(), True),
        StructField("surname", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

_TEAM_SCHEMA = StructType(
    [
        StructField("constructorsChampionships", LongType(), True),
        StructField("driversChampionships", LongType(), True),
        StructField("firstAppareance", LongType(), True),
        StructField("nationality", StringType(), True),
        StructField("teamId", StringType(), True),
        StructField("teamName", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

_RESULT_SCHEMA = StructType(
    [
        StructField("driver", _DRIVER_SCHEMA, True),
        StructField("fastLap", StringType(), True),
        StructField("grid", StringType(), True),
        StructField("points", LongType(), True),
        StructField("position", StringType(), True),
        StructField("retired", StringType(), True),
        StructField("team", _TEAM_SCHEMA, True),
        StructField("time", StringType(), True),
    ]
)

_RACES_SCHEMA = StructType(
    [
        StructField("circuit", ArrayType(_CIRCUIT_SCHEMA), True),
        StructField("date", StringType(), True),
        StructField("raceId", StringType(), True),
        StructField("raceName", StringType(), True),
        StructField("results", ArrayType(_RESULT_SCHEMA), True),
        StructField("round", StringType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

RAW_RACE_SCHEMA = StructType(
    [
        StructField("api", StringType(), True),
        StructField("limit", LongType(), True),
        StructField("offset", LongType(), True),
        StructField("races", _RACES_SCHEMA, True),
        StructField("season", LongType(), True),
        StructField("total", LongType(), True),
        StructField("url", StringType(), True),
    ]
)
