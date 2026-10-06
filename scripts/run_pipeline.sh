#!/usr/bin/env bash
# Trigger a StartJobRun for the deployed F1 SDP pipeline, for one (season, round).
#
#   scripts/run_pipeline.sh --profile <your-aws-profile> [--region <region>] \
#     --season <year> --round <round> \
#     [--mode validate|run] [--full-refresh <dataset>|--full-refresh-all] [--wait]
#
# --mode defaults to "run". VALIDATE performs a dry run (dependency/SQL/Python
# compile checks) without writing data -- note the bronze API fetch itself likely
# still executes during VALIDATE, since only writes are suppressed.
# --full-refresh/--full-refresh-all reset streaming-table checkpoints and
# reprocess; only one of the two may be given, and only with --mode run.
# --wait polls until the run leaves RUNNING/STARTING/STOPPING and exits non-zero
# if it didn't SUCCEED.
#
# One run = one race, same granularity as the Databricks original this pipeline
# ports: populating multiple races means repeated invocations with different
# --season/--round values.
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

SEASON=""
ROUND=""
MODE="run"
FULL_REFRESH=""
WAIT=false
COMMON_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --season) SEASON="${2:?--season needs a value}"; shift 2 ;;
    --round) ROUND="${2:?--round needs a value}"; shift 2 ;;
    --mode) MODE="${2:?--mode needs a value}"; shift 2 ;;
    --full-refresh) FULL_REFRESH="--full-refresh ${2:?--full-refresh needs a dataset name}"; shift 2 ;;
    --full-refresh-all) FULL_REFRESH="--full-refresh-all"; shift ;;
    --wait) WAIT=true; shift ;;
    -p|--profile|-r|--region) COMMON_ARGS+=("$1" "$2"); shift 2 ;;
    *) echo "error: unrecognized argument: $1" >&2; exit 1 ;;
  esac
done
parse_common_args "${COMMON_ARGS[@]+"${COMMON_ARGS[@]}"}"

[[ -n "$SEASON" && -n "$ROUND" ]] || { echo "error: --season and --round are both required." >&2; exit 1; }
case "$MODE" in
  validate|run) ;;
  *) echo "error: --mode must be 'validate' or 'run' (got: $MODE)" >&2; exit 1 ;;
esac

JOB_NAME="$(stack_output JobName)"
if [[ -z "$JOB_NAME" || "$JOB_NAME" == "None" ]]; then
  echo "error: no JobName output for stack $STACK_NAME; deploy must have failed." >&2
  exit 1
fi

# Glue's --arguments is a flat JSON map, so jobMode/spark.f1.season.*/runMode all
# ride inside a single combined --conf value. Keys are "spark."-prefixed: Spark's
# --conf parser silently drops any key that doesn't start with "spark." (Apache
# Spark SPARK-7037) -- confirmed the hard way when these keys were plain
# "f1.season.year"/"f1.season.round" and never reached spark.conf.get() on a
# real Glue run.
CONF="spark.glue.sdp.jobMode=$(tr '[:lower:]' '[:upper:]' <<<"$MODE") --conf spark.f1.season.year=$SEASON --conf spark.f1.season.round=$ROUND"
if [[ -n "$FULL_REFRESH" ]]; then
  CONF="$CONF --conf spark.glue.sdp.runMode=$FULL_REFRESH"
fi

echo "Starting job $JOB_NAME (season=$SEASON round=$ROUND mode=$MODE${FULL_REFRESH:+ $FULL_REFRESH})..."
RUN_ID="$(aws glue start-job-run --profile "$AWS_PROFILE" --region "$AWS_REGION" \
  --job-name "$JOB_NAME" --arguments "{\"--conf\":\"$CONF\"}" --query JobRunId --output text)"
echo "RunId: $RUN_ID"

if ! $WAIT; then
  exit 0
fi

echo "Waiting for run to finish..."
while true; do
  STATE="$(aws glue get-job-run --profile "$AWS_PROFILE" --region "$AWS_REGION" \
    --job-name "$JOB_NAME" --run-id "$RUN_ID" --query 'JobRun.JobRunState' --output text)"
  case "$STATE" in
    STARTING|RUNNING|STOPPING) sleep 10 ;;
    SUCCEEDED) echo "Run $RUN_ID: $STATE"; exit 0 ;;
    *) echo "Run $RUN_ID: $STATE"; exit 1 ;;
  esac
done
