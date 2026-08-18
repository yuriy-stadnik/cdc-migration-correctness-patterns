#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic> [max_messages] [timeout_ms]" >&2
  exit 1
fi

MAX_MESSAGES="${2:-20}"
TIMEOUT_MS="${3:-15000}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

MSK_BOOTSTRAP="$(terraform_output msk_bootstrap_sasl_iam)"
TOPIC_B64="$(printf '%s' "$1" | base64)"

{
  printf 'MSK_BOOTSTRAP=%q\n' "${MSK_BOOTSTRAP}"
  printf 'TOPIC_B64=%q\n' "${TOPIC_B64}"
  printf 'MAX_MESSAGES=%q\n' "${MAX_MESSAGES}"
  printf 'TIMEOUT_MS=%q\n' "${TIMEOUT_MS}"
  cat <<'REMOTE'
set -euo pipefail
TOPIC="$(printf '%s' "${TOPIC_B64}" | base64 --decode)"
sudo docker exec lab-mirrormaker2-1 kafka-console-consumer \
  --bootstrap-server "${MSK_BOOTSTRAP}" \
  --consumer.config /tmp/client.properties \
  --topic "${TOPIC}" \
  --from-beginning \
  --timeout-ms "${TIMEOUT_MS}" \
  --max-messages "${MAX_MESSAGES}" \
  --property print.key=true
REMOTE
} | run_script_on_ec2
