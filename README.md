# f1-glue-sdp

An F1 racing analytics pipeline built on **AWS Glue 6.0 Spark Declarative Pipelines (SDP)** --
a port of an existing [Databricks Lakeflow Declarative Pipelines implementation](https://docs.aws.amazon.com/glue/latest/dg/spark-declarative-pipelines.html)
of the same use case, adapted to AWS Glue's declarative-pipeline framework and its
constraints.

## Architecture

```
F1 API (f1api.dev)
      │  requests.get() inside the bronze flow -- no separate ingestion step
      ▼
┌─────────────────────────── bronze (streaming tables) ───────────────────────────┐
│ raw_race → race_results_unnested                                                 │
│                               ├→ race_drivers                                   │
│                               ├→ race_teams                                     │
│                               └→ race_circuits                                  │
└───────────────────────────────────────────────────────────────────────────────┘
      ▼
┌──────────────────────── curated (materialized views, SCD1) ─────────────────────┐
│ dim_drivers · dim_teams · dim_circuits · fact_race_results                       │
│ (latest row per key via ROW_NUMBER() -- no SCD2/history; Glue SDP has no         │
│  create_auto_cdc_flow/APPLY CHANGES primitive)                                  │
└───────────────────────────────────────────────────────────────────────────────┘
      ▼
┌──────────────────────── gold (materialized views) ──────────────────────────────┐
│ driver_performance_summary · team_performance_summary · circuit_summary         │
└───────────────────────────────────────────────────────────────────────────────┘
```

All of this runs as **one Glue 6.0 job** with SDP enabled -- the framework infers the
full bronze → curated → gold dependency graph from table references in
`pipeline_src/transformations/`, so there's no separate orchestration resource.

Key differences from the Databricks original (see `pipeline_src/f1_pipeline_lib/` and
`pipeline_src/transformations/` for the full rationale in code comments):

- **Bronze calls the F1 API directly** (`f1_pipeline_lib/extract.py`), instead of a
  separate ingestion notebook dumping JSON files for an Auto Loader stream to read.
- **No SCD Type 2.** AWS Glue 6.0 SDP has no CDC/MERGE/upsert primitive (confirmed from
  the official docs: only streaming tables and materialized views exist as writable
  dataset types). All four curated tables are **SCD Type 1** (latest-state-only),
  resolved via a `ROW_NUMBER()` window function over the full bronze history.
- **No quarantine/data-quality layer in v1** (deferred). A malformed API response or
  network failure simply fails the job run; re-run it.

## Repo layout

| Path | What |
|---|---|
| `app.py`, `cdk.json` | CDK entrypoint, context-driven naming/sizing knobs |
| `f1_glue_sdp/` | The one CDK stack (S3 bucket, IAM role, Glue database, Glue job) |
| `pipeline_src/spark-pipeline.yml` | SDP manifest template (Iceberg + Glue Catalog config) |
| `pipeline_src/f1_pipeline_lib/` | Plain Python (no `pyspark.pipelines` import) -- HTTP fetch + schema. Independently unit-testable. |
| `pipeline_src/transformations/` | The SDP pipeline itself: bronze streaming tables (incl. the explode/flatten logic, which lives here rather than in `f1_pipeline_lib/` since it's only ever called once per table and needs an active SDP graph context anyway), curated/gold materialized views |
| `scripts/` | Deploy, destroy, package, and run-a-race helpers |
| `tests/` | CDK infra assertions, packaging tests, and `f1_pipeline_lib` unit tests |

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate   # or `source .venv/Scripts/activate.bat` equivalent on Windows via source.bat
pip install -r requirements.txt -r requirements-dev.txt
```

Running `f1_pipeline_lib`'s tests locally needs **Java 17+** (Glue 6.0 runs Spark 4.1.1,
which requires it) -- e.g. `brew install openjdk@17` and point `JAVA_HOME` at it.

Run the test suite:

```bash
pytest tests/
```

## Deploy

```bash
scripts/deploy.sh --profile <your-aws-profile> --bootstrap   # --bootstrap only needed once per account/region
```

`cdk.json`'s `context` block pins the bucket name/suffix, prefix, database, and job
name so re-synthesizing doesn't change them. Override any of them per-run with
`-c key=value -- ` appended to `deploy.sh`, e.g.:

```bash
scripts/deploy.sh --profile my-profile -- -c environment_tag=prod
```

## Run a race

Bronze pulls exactly one race (one `season`/`round`) per job run -- same granularity
as the Databricks original. Populating more history means repeated runs with
different values.

```bash
# Dry run: checks dependency/SQL/Python compilation, writes no data (the bronze API
# fetch itself likely still executes -- only writes are suppressed).
scripts/run_pipeline.sh --profile <your-aws-profile> --season 2024 --round 1 --mode validate --wait

# Real run
scripts/run_pipeline.sh --profile <your-aws-profile> --season 2024 --round 1 --mode run --wait

# Another race, to see the pipeline accumulate history
scripts/run_pipeline.sh --profile <your-aws-profile> --season 2024 --round 2 --mode run --wait

# Force a full recompute of one dataset (e.g. after a logic change)
scripts/run_pipeline.sh --profile <your-aws-profile> --season 2024 --round 1 --full-refresh dim_drivers
# ...or everything
scripts/run_pipeline.sh --profile <your-aws-profile> --season 2024 --round 1 --full-refresh-all
```

## Verify

```bash
aws glue get-tables --database-name f1_sdp_db --profile <your-aws-profile> \
  --query 'TableList[].Name'
```

Then query via Athena, e.g.:

```sql
SELECT * FROM f1_sdp_db.raw_race;
SELECT COUNT(*) FROM f1_sdp_db.race_drivers;
SELECT * FROM f1_sdp_db.driver_performance_summary ORDER BY wins DESC LIMIT 10;
```

## Teardown

```bash
scripts/destroy.sh --profile <your-aws-profile>
```

The bucket uses `auto_delete_objects`, so Iceberg warehouse data and SDP checkpoint
state are deleted along with the stack.

## Known limitations (v1)

- No quarantine/data-quality layer -- a bad API response or parse failure fails the
  whole job run rather than being routed anywhere. Deferred; revisit later.
- No multi-race/season backfill -- one run processes exactly one race.
- No SCD Type 2 / history tracking on dimensions (see Architecture above).
