#!/usr/bin/env bash
# Deploy the CDK stack with a chosen AWS profile.
#
#   scripts/deploy.sh --profile <your-aws-profile> [--region <region>] [--bootstrap] \
#     [-- <extra cdk args>]
#
# --bootstrap runs `cdk bootstrap` first (needed once per account/region).
# Extra args after `--` go to `cdk deploy` untouched, e.g. -- -c environment_tag=prod
#
# There is no --upload step here: bronze pulls from the F1 API directly, so there's
# no input file to stage in S3. After deploying, run a race with scripts/run_pipeline.sh.
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
COMMON_ARGS=()
set -- "${OWN_ARGS[@]+"${OWN_ARGS[@]}"}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap) BOOTSTRAP=true; shift ;;
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

echo
echo "Next: scripts/run_pipeline.sh --profile $AWS_PROFILE --season 2024 --round 1 --mode validate --wait"
