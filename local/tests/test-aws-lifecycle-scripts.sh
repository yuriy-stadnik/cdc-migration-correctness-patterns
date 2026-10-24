#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/aws-lifecycle-test.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

FAKE_TERRAFORM="$TMP_DIR/terraform"
LOG_FILE="$TMP_DIR/terraform.log"
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
    if [[ "${TF_FAKE_MODE:-}" == "partial-down" && ! -f "$TF_FAKE_DIR/destroyed" ]]; then
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
    if [[ "$count" -ge 2 ]]; then
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

run_with_fake_terraform() {
  env \
    TERRAFORM_BIN="$FAKE_TERRAFORM" \
    TF_FAKE_LOG="$LOG_FILE" \
    TF_FAKE_DIR="$TMP_DIR" \
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
