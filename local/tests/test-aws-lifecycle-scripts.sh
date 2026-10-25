#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/aws-lifecycle-test.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

FAKE_TERRAFORM="$TMP_DIR/terraform"
FAKE_AWS="$TMP_DIR/aws"
LOG_FILE="$TMP_DIR/terraform.log"
AWS_LOG_FILE="$TMP_DIR/aws.log"
PROJECTOR_JAR="$TMP_DIR/projector.jar"
LAMBDA_JAR="$TMP_DIR/lambda.jar"
touch "$PROJECTOR_JAR" "$LAMBDA_JAR"

cat > "$FAKE_TERRAFORM" <<'FAKE_TERRAFORM_SCRIPT'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$TF_FAKE_LOG"
command_name=""
for argument in "$@"; do
  case "$argument" in
    init|validate|apply|destroy|output|state) command_name="$argument"; break ;;
  esac
done

case "$command_name" in
  init|validate|output) exit 0 ;;
  state)
    if [[ "$*" == *' state show '* ]]; then
      case "${@: -1}" in
        'aws_subnet.private[0]') echo '    id = "subnet-test"' ;;
        'aws_security_group.msk') echo '    id = "sg-test"' ;;
        *) exit 1 ;;
      esac
    elif [[ "${TF_FAKE_MODE:-}" == "partial-down" && ! -f "$TF_FAKE_DIR/destroyed" ]]; then
      echo 'aws_vpc.this'
    elif [[ "${TF_FAKE_MODE:-}" == "orphan-down" && ! -f "$TF_FAKE_DIR/destroyed" ]]; then
      echo 'data.aws_availability_zones.available'
      echo 'aws_security_group.msk'
      echo 'aws_subnet.private[0]'
      echo 'aws_vpc.this'
    elif [[ "${TF_FAKE_MODE:-}" == "up" ]]; then
      echo 'aws_vpc.this'
    elif [[ "${TF_FAKE_MODE:-}" == "complete-up" ]]; then
      echo 'aws_lambda_event_source_mapping.msk_operational["cdc.tx.control.v2"]'
    fi
    ;;
  destroy)
    count=0
    [[ -f "$TF_FAKE_DIR/destroy-attempts" ]] && count="$(cat "$TF_FAKE_DIR/destroy-attempts")"
    count=$((count + 1))
    printf '%s\n' "$count" > "$TF_FAKE_DIR/destroy-attempts"
    if [[ "${TF_FAKE_MODE:-}" == "orphan-down" && -f "$AWS_FAKE_DIR/eni-deleted" ]]; then
      touch "$TF_FAKE_DIR/destroyed"
    elif [[ "${TF_FAKE_MODE:-}" != "orphan-down" && "$count" -ge 2 ]]; then
      touch "$TF_FAKE_DIR/destroyed"
    else
      exit 1
    fi
    ;;
  apply)
    if [[ "$*" == *'enable_msk_event_source_mappings=true'* ]]; then
      [[ "${TF_FAKE_MODE:-}" == "complete-up" ]] && exit 0
      count=0
      [[ -f "$TF_FAKE_DIR/mapping-attempts" ]] && count="$(cat "$TF_FAKE_DIR/mapping-attempts")"
      count=$((count + 1))
      printf '%s\n' "$count" > "$TF_FAKE_DIR/mapping-attempts"
      [[ "$count" -ge 2 ]]
    fi
    ;;
  *) exit 1 ;;
esac
FAKE_TERRAFORM_SCRIPT
chmod +x "$FAKE_TERRAFORM"

cat > "$FAKE_AWS" <<'FAKE_AWS_SCRIPT'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$AWS_FAKE_LOG"
case "$*" in
  *'describe-network-interfaces'*'Name=subnet-id,Values=subnet-test'*)
    [[ -f "$AWS_FAKE_DIR/eni-deleted" ]] || printf '%s\n' 'eni-orphan-test'
    ;;
  *'describe-network-interfaces'*'--network-interface-ids eni-orphan-test'*)
    printf '%s\n' "${AWS_FAKE_GROUP_ID:-sg-test}"
    ;;
  *'delete-network-interface'*'--network-interface-id eni-orphan-test'*)
    touch "$AWS_FAKE_DIR/eni-deleted"
    ;;
  *) exit 1 ;;
esac
FAKE_AWS_SCRIPT
chmod +x "$FAKE_AWS"

run_with_fake_terraform() {
  env \
    TERRAFORM_BIN="$FAKE_TERRAFORM" \
    TF_FAKE_LOG="$LOG_FILE" \
    TF_FAKE_DIR="$TMP_DIR" \
    AWS_BIN="$FAKE_AWS" \
    AWS_FAKE_LOG="$AWS_LOG_FILE" \
    AWS_FAKE_DIR="$TMP_DIR" \
    PROJECTOR_JAR="$PROJECTOR_JAR" \
    LAMBDA_JAR="$LAMBDA_JAR" \
    SKIP_BUILD=1 \
    AUTO_APPROVE=1 \
    RETRY_DELAY_SECONDS=0 \
    TOPIC_INIT_SETTLE_SECONDS=0 \
    "$@"
}

: > "$LOG_FILE"
TF_FAKE_MODE=no-state run_with_fake_terraform ./scripts/aws-down.sh us-east-1
if grep -q 'destroy ' "$LOG_FILE"; then
  echo "aws-down attempted destroy with empty state" >&2
  exit 1
fi

: > "$LOG_FILE"
rm -f "$TMP_DIR/destroyed" "$TMP_DIR/destroy-attempts"
TF_FAKE_MODE=partial-down run_with_fake_terraform ./scripts/aws-down.sh us-east-1
grep -F 'destroy -input=false -auto-approve' "$LOG_FILE" >/dev/null
test -f "$TMP_DIR/destroyed"
test "$(cat "$TMP_DIR/destroy-attempts")" = "2"

: > "$LOG_FILE"
: > "$AWS_LOG_FILE"
rm -f "$TMP_DIR/destroyed" "$TMP_DIR/destroy-attempts" "$TMP_DIR/eni-deleted"
if TF_FAKE_MODE=orphan-down run_with_fake_terraform ./scripts/aws-down.sh us-east-1 >"$TMP_DIR/orphan-default.out" 2>&1; then
  echo "aws-down continued without explicit orphan ENI cleanup opt-in" >&2
  exit 1
fi
grep -F 'eni-orphan-test' "$TMP_DIR/orphan-default.out" >/dev/null
grep -F 'CLEANUP_ORPHAN_ENIS=1' "$TMP_DIR/orphan-default.out" >/dev/null
grep -F 'Status==`available` && RequesterManaged==`false` && Attachment==null' "$AWS_LOG_FILE" >/dev/null
if grep -F 'delete-network-interface' "$AWS_LOG_FILE" >/dev/null; then
  echo "aws-down deleted an orphan ENI without explicit opt-in" >&2
  exit 1
fi
if grep -q 'destroy ' "$LOG_FILE"; then
  echo "aws-down started a long destroy retry after detecting an orphan ENI" >&2
  exit 1
fi

: > "$LOG_FILE"
: > "$AWS_LOG_FILE"
rm -f "$TMP_DIR/destroyed" "$TMP_DIR/destroy-attempts" "$TMP_DIR/eni-deleted"
TF_FAKE_MODE=orphan-down CLEANUP_ORPHAN_ENIS=1 run_with_fake_terraform ./scripts/aws-down.sh us-east-1
grep -F 'delete-network-interface --region us-east-1 --network-interface-id eni-orphan-test' "$AWS_LOG_FILE" >/dev/null
test -f "$TMP_DIR/eni-deleted"
test -f "$TMP_DIR/destroyed"
test "$(cat "$TMP_DIR/destroy-attempts")" = "1"

: > "$LOG_FILE"
: > "$AWS_LOG_FILE"
rm -f "$TMP_DIR/destroyed" "$TMP_DIR/destroy-attempts" "$TMP_DIR/eni-deleted"
if TF_FAKE_MODE=orphan-down AWS_FAKE_GROUP_ID=sg-foreign CLEANUP_ORPHAN_ENIS=1 \
  run_with_fake_terraform ./scripts/aws-down.sh us-east-1 >"$TMP_DIR/orphan-foreign.out" 2>&1; then
  echo "aws-down continued after finding an ENI with a non-project security group" >&2
  exit 1
fi
grep -F 'outside the remaining Terraform state' "$TMP_DIR/orphan-foreign.out" >/dev/null
if grep -F 'delete-network-interface' "$AWS_LOG_FILE" >/dev/null; then
  echo "aws-down deleted an ENI with a non-project security group" >&2
  exit 1
fi

: > "$LOG_FILE"
rm -f "$TMP_DIR/mapping-attempts"
TF_FAKE_MODE=up MAPPING_APPLY_ATTEMPTS=3 run_with_fake_terraform \
  ./scripts/aws-up.sh us-east-1 203.0.113.10
grep -F 'enable_msk_event_source_mappings=false' "$LOG_FILE" >/dev/null
grep -F 'enable_msk_event_source_mappings=true' "$LOG_FILE" >/dev/null
test "$(cat "$TMP_DIR/mapping-attempts")" = "2"

: > "$LOG_FILE"
TF_FAKE_MODE=complete-up run_with_fake_terraform ./scripts/aws-up.sh us-east-1 203.0.113.10
if grep -F 'enable_msk_event_source_mappings=false' "$LOG_FILE" >/dev/null; then
  echo "aws-up disabled existing mappings during a convergent rerun" >&2
  exit 1
fi
grep -F 'enable_msk_event_source_mappings=true' "$LOG_FILE" >/dev/null

grep -F 'force_destroy = true' artifacts.tf >/dev/null
echo "AWS lifecycle scripts: ok"
