#!/usr/bin/env bash
# Deploy the CDK stack with a chosen AWS profile.
#
#   scripts/deploy.sh --profile <your-aws-profile> [--region <region>] [--bootstrap] \
#     [--upload-reference-data [--reference-data-source <path>] \
#                               [--reference-data-prefix <s3-key-prefix>]] \
#     [-- <extra cdk args>]
#
# --bootstrap runs `cdk bootstrap` first (needed once per account/region).
# Extra args after `--` go to `cdk deploy` untouched, e.g. -- -c environment_tag=prod
#
# `cdk deploy` itself never uploads anything from reference_data/ at the repo root --
# deliberately kept out of the CDK stack so a plain deploy can't accidentally ship
# stray local files (e.g. .DS_Store) to S3. Pass --upload-reference-data to sync
# that folder's CSVs (and only those) to S3 for the circuit_track_type streaming
# table to read.
#
# By default the whole reference_data/ folder is synced to
# s3://<bucket>/<stack-prefix>/reference-data/ (the path spark-pipeline.yml's
# spark.f1.reference.circuit_track_type.path already points at). Two flags
# narrow that down independently:
#
#   --reference-data-source <path>  A single .csv file or a subdirectory, instead
#                                    of the whole reference_data/ folder. Relative
#                                    paths resolve against the repo root. A
#                                    directory is synced (with --delete, so it
#                                    stays an exact mirror); a single file is
#                                    copied on its own (no deletion of siblings).
#   --reference-data-prefix <prefix> Destination S3 key prefix, same bucket,
#                                    instead of <stack-prefix>/reference-data.
#
# Combine them to stage one specific file at one specific prefix, e.g. to push
# just the incremental-load demo file on its own:
#
#   scripts/deploy.sh --profile my-profile --upload-reference-data \
#     --reference-data-source reference_data/circuit_track_type/incremental_load.csv \
#     --reference-data-prefix f1-pipeline/reference-data/circuit_track_type
#
# If you point --reference-data-prefix anywhere other than the default, also
# update spark-pipeline.yml's spark.f1.reference.circuit_track_type.path to
# match -- these flags only control what gets uploaded and where, not where
# the Glue job reads from.
#
# Bronze's main raw_race source still needs no upload step at all: it pulls from
# the F1 API directly. After deploying, run a race with scripts/run_pipeline.sh.
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# Split at the first literal "--": everything before is our own flags/profile;
# everything after is forwarded to `cdk deploy` untouched.
OWN_ARGS=()
CDK_EXTRA=()
SEEN_SEP=false
for a in "$@"; do
  if ! $SEEN_SEP && [[ "$a" == "--" ]]; then
    SEEN_SEP=true
    continue
  fi
  if $SEEN_SEP; then
    CDK_EXTRA+=("$a")
  else
    OWN_ARGS+=("$a")
  fi
done

BOOTSTRAP=false
UPLOAD_REFERENCE_DATA=false
REFERENCE_DATA_SOURCE=""
REFERENCE_DATA_PREFIX=""
COMMON_ARGS=()
set -- "${OWN_ARGS[@]+"${OWN_ARGS[@]}"}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap) BOOTSTRAP=true; shift ;;
    --upload-reference-data) UPLOAD_REFERENCE_DATA=true; shift ;;
    --reference-data-source)
      UPLOAD_REFERENCE_DATA=true
      REFERENCE_DATA_SOURCE="${2:?--reference-data-source needs a value}"
      shift 2 ;;
    --reference-data-prefix)
      UPLOAD_REFERENCE_DATA=true
      REFERENCE_DATA_PREFIX="${2:?--reference-data-prefix needs a value}"
      shift 2 ;;
    -p|--profile|-r|--region) COMMON_ARGS+=("$1" "$2"); shift 2 ;;
    *) echo "error: unrecognized argument: $1" >&2; exit 1 ;;
  esac
done
parse_common_args "${COMMON_ARGS[@]+"${COMMON_ARGS[@]}"}"

cd "$PROJECT_ROOT"
activate_venv

if $BOOTSTRAP; then
  cdk bootstrap "aws://$CDK_DEFAULT_ACCOUNT/$AWS_REGION" --profile "$AWS_PROFILE"
fi

cdk deploy --profile "$AWS_PROFILE" --require-approval never "${CDK_EXTRA[@]+"${CDK_EXTRA[@]}"}"

echo
echo "Deployed. Stack outputs:"
for k in BucketName Prefix DatabaseName JobName GlueJobRoleArn; do
  printf '  %-15s %s\n' "$k" "$(stack_output "$k")"
done

if $UPLOAD_REFERENCE_DATA; then
  BUCKET="$(stack_output BucketName)"
  STACK_PREFIX="$(stack_output Prefix)"
  # Default matches spark-pipeline.yml's
  # spark.f1.reference.circuit_track_type.path; --reference-data-prefix
  # overrides just the destination key prefix, same bucket.
  DEST_PREFIX="${REFERENCE_DATA_PREFIX:-$STACK_PREFIX/reference-data}"
  DEST_PREFIX="${DEST_PREFIX#/}"   # tolerate a leading/trailing slash either way
  DEST_PREFIX="${DEST_PREFIX%/}"
  DEST="s3://$BUCKET/$DEST_PREFIX/"

  # Default matches the whole reference_data/ folder; --reference-data-source
  # overrides it to one specific file or subdirectory. Relative paths resolve
  # against the repo root, so callers can write e.g.
  # "reference_data/circuit_track_type/incremental_load.csv" regardless of cwd.
  SRC="${REFERENCE_DATA_SOURCE:-$PROJECT_ROOT/reference_data}"
  if [[ "$SRC" != /* ]]; then
    SRC="$PROJECT_ROOT/$SRC"
  fi

  if [[ -n "$REFERENCE_DATA_PREFIX" ]]; then
    PREFIX_NOTE=" (custom prefix -- make sure spark.f1.reference.circuit_track_type.path in spark-pipeline.yml points here too, or the Glue job won't read it)"
  else
    PREFIX_NOTE=""
  fi

  echo
  if [[ -d "$SRC" ]]; then
    echo "Uploading $SRC/ -> $DEST$PREFIX_NOTE"
    # --exclude/--include order matters: exclude everything first, then
    # re-include just the data files, so stray local junk (.DS_Store,
    # __pycache__, etc.) never reaches S3 even if it shows up in the folder
    # later. --delete keeps this an exact mirror of $SRC.
    aws s3 sync "$SRC/" "$DEST" \
      --profile "$AWS_PROFILE" --delete \
      --exclude "*" \
      --include "*.csv"
  elif [[ -f "$SRC" ]]; then
    case "$SRC" in
      *.csv) ;;
      *)
        echo "error: --reference-data-source must be a .csv file or a directory: $SRC" >&2
        exit 1 ;;
    esac
    # A single-file upload is a plain copy, not a sync -- it must never delete
    # any sibling object already at this prefix (e.g. a file staged there by a
    # previous --reference-data-source run).
    echo "Uploading $SRC -> $DEST$(basename "$SRC")$PREFIX_NOTE"
    aws s3 cp "$SRC" "$DEST$(basename "$SRC")" --profile "$AWS_PROFILE"
  else
    echo "error: --reference-data-source path not found: $SRC" >&2
    exit 1
  fi
fi

echo
echo "Next: scripts/run_pipeline.sh --profile $AWS_PROFILE --season 2024 --round 1 --mode validate --wait"
