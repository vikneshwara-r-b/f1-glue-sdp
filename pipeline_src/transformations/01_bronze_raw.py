import sys
from pathlib import Path

# Defensive import-path guard: ensures the zip root (where f1_pipeline_lib/ sits
# alongside transformations/) is importable even if Glue's SDP runtime doesn't
# already put the zip root on sys.path.
_ZIP_ROOT = Path(__file__).resolve().parent.parent
if str(_ZIP_ROOT) not in sys.path:
    sys.path.insert(0, str(_ZIP_ROOT))

from f1_pipeline_lib.extract import ensure_staging_path_exists, fetch_race_payload, stage_payload
from f1_pipeline_lib.schema import RAW_RACE_SCHEMA

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# Explicit output schema for raw_race, matching exactly what ingest_raw_race()
# below returns: RAW_RACE_SCHEMA's fields plus the four lineage columns appended
# in the final .select(...). Needed because raw_race is the one table in this
# pipeline that's referenced downstream (02_bronze_prepared.py's four flows all
# do spark.readStream.table("raw_race")) without ever being given its own
# declared schema -- confirmed on a real Glue VALIDATE run that this breaks:
# the server-side dependency analyzer, with no declared schema to go on, tries
# to resolve raw_race against the real Glue Catalog table instead of the
# registered flow's own plan, and fails with
# [TABLE_OR_VIEW_NOT_FOUND] on a brand-new deployment where raw_race has never
# actually been created yet (zero prior successful RUNs). Passing a StructType
# here (not a DDL string) is the same safe pattern used everywhere else --
# confirmed from pyspark/pipelines/spark_connect_graph_element_registry.py that
# a StructType converts to schema_data_type via pure local protobuf conversion,
# no RPC, whereas a string would just travel as schema_string -- either is
# client-side-safe here, but StructType stays consistent with the rest of this
# file.
_RAW_RACE_OUTPUT_SCHEMA = StructType(
    list(RAW_RACE_SCHEMA.fields)
    + [
        # nullable=False on all four: F.lit() on a literal string and
        # F.current_timestamp() both produce non-null columns (verified against
        # the actual runtime schema of ingest_raw_race()'s return value) -- must
        # match exactly, since this declared schema is what the server-side
        # analyzer trusts instead of resolving the real table.
        StructField("source_url", StringType(), False),
        StructField("season_requested", StringType(), False),
        StructField("round_requested", StringType(), False),
        StructField("load_date_time", TimestampType(), False),
    ]
)

# Custom Spark conf keys MUST be prefixed with "spark." -- Spark's spark-submit
# argument parser silently drops any --conf key that doesn't start with "spark."
# (Apache Spark SPARK-7037), so a key like "f1.season.year" never reaches
# spark.conf.get() at all when passed via `StartJobRun --arguments`. Confirmed by
# a real failure on Glue: spark.conf.get("f1.season.year", None) came back None
# even though scripts/run_pipeline.sh passed --conf f1.season.year=<value>.
BASE_URL_CONF = "spark.f1.api.base_url"
SEASON_CONF = "spark.f1.season.year"
ROUND_CONF = "spark.f1.season.round"
STAGING_PATH_CONF = "spark.f1.staging.path"
DEFAULT_BASE_URL = "https://f1api.dev/api"

# spark.glue.sdp.jobMode is the same --conf key scripts/run_pipeline.sh already
# sets on every invocation (VALIDATE or RUN) to control Glue's own dry-run
# behavior. It's spark.-prefixed, so -- unlike the internal `dry` flag inside
# pyspark.pipelines itself, which never reaches user code (confirmed by reading
# that package's source directly) -- this one genuinely lands in the session's
# conf and is readable here. No new custom flag/no run_pipeline.sh change needed.
JOB_MODE_CONF = "spark.glue.sdp.jobMode"

# Streaming table: AWS Glue SDP's documented Python API for incremental tables is
# dp.create_streaming_table() + @dp.append_flow(target=...), not a @dp.table decorator.
# https://docs.aws.amazon.com/glue/latest/dg/spark-declarative-pipelines.html
dp.create_streaming_table(
    "raw_race",
    comment=(
        "One row per pipeline run: the F1 API payload for the requested "
        "(season, round), parsed into RAW_RACE_SCHEMA."
    ),
    schema=_RAW_RACE_OUTPUT_SCHEMA,
)


@dp.append_flow(target="raw_race")
def ingest_raw_race():
    # Glue's SDP wrapper doesn't inject a `spark` global into pipeline files.
    spark = SparkSession.active()
    base_url = spark.conf.get(BASE_URL_CONF, DEFAULT_BASE_URL)
    season = spark.conf.get(SEASON_CONF, None)  # required -- StartJobRun --conf, no silent default
    round_ = spark.conf.get(ROUND_CONF, None)  # required -- StartJobRun --conf, no silent default
    staging_path = spark.conf.get(STAGING_PATH_CONF, None)  # required -- set in spark-pipeline.yml
    job_mode = spark.conf.get(JOB_MODE_CONF, "RUN")

    # VALIDATE is a dry run that's documented to write no data -- but SDP still
    # executes this whole function body during VALIDATE regardless (confirmed:
    # flow.func() is called unconditionally during registration, before the dry
    # flag is even in scope). Without this guard, every VALIDATE run called the
    # live F1 API and staged a real file to S3 for no benefit, since VALIDATE
    # never materializes raw_race anyway. Skipping the fetch+stage here doesn't
    # weaken VALIDATE's actual checks: schema/dependency resolution only needs
    # the streaming-read plan below, which is built the same way either way.
    if job_mode.upper() != "VALIDATE":
        # Raises on a missing conf, network/HTTP failure, or non-2xx response --
        # there is no quarantine/retry layer in v1 (explicit decision, revisit
        # later), so a bad fetch simply fails the job run rather than being
        # caught and routed anywhere.
        payload_text = fetch_race_payload(base_url, season, round_)

        # raw_race is a streaming table, so this flow must return a genuine
        # streaming relation -- confirmed on a real Glue RUN-mode execution that
        # a batch DataFrame built in-process (via createDataFrame/from_json) is
        # rejected: AnalysisException
        # [INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE].
        #
        # (A custom PySpark Python Data Source was tried as an alternative to
        # this S3-staging step -- confirmed on a real Glue run to be currently
        # broken on Glue 6.0 for unrelated platform reasons: the Python Data
        # Source worker runs under Python 3.9 internally, but Glue's bundled
        # pyspark.zip's sql/types.py needs Python 3.10+ syntax to even import.
        # Reverted; staging remains the known-working mechanism.)
        #
        # stage_payload is pure boto3 (no Spark API, so no risk of tripping the
        # pipeline-query-function analyze/execute block) -- it writes this run's
        # payload to a unique key under staging_path, which the streaming read
        # below then picks up as a new file. Spark's own file-source
        # checkpointing (under this pipeline's `storage:` location) tracks
        # which files have already been processed, so re-running never
        # reprocesses an old file.
        stage_payload(payload_text, staging_path, season, round_)
    else:
        # Guarantees staging_path exists even if nothing has ever been staged
        # there yet (e.g. a VALIDATE run against a brand-new deployment) --
        # confirmed on a real Glue run that without this, the streaming read
        # below raises AnalysisException [PATH_NOT_FOUND] at plan-build time.
        ensure_staging_path_exists(staging_path)

    parsed = (
        spark.readStream.schema(RAW_RACE_SCHEMA)
        .option("pathGlobFilter", "*.json")
        .json(staging_path)
    )
    return parsed.select(
        "*",
        F.lit(f"{base_url}/{season}/{round_}/race").alias("source_url"),
        F.lit(season).alias("season_requested"),
        F.lit(round_).alias("round_requested"),
        F.current_timestamp().alias("load_date_time"),
    )
