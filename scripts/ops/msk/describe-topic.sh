#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic>" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

MSK_BOOTSTRAP="$(terraform_output msk_bootstrap_sasl_iam)"
TOPIC_B64="$(printf '%s' "$1" | base64)"

{
  printf 'MSK_BOOTSTRAP=%q\n' "${MSK_BOOTSTRAP}"
  printf 'TOPIC_B64=%q\n' "${TOPIC_B64}"
  cat <<'REMOTE'
set -euo pipefail
TOPIC="$(printf '%s' "${TOPIC_B64}" | base64 --decode)"
sudo docker exec lab-mirrormaker2-1 kafka-topics \
  --bootstrap-server "${MSK_BOOTSTRAP}" \
  --command-config /tmp/client.properties \
  --describe \
  --topic "${TOPIC}"
REMOTE
} | run_script_on_ec2
