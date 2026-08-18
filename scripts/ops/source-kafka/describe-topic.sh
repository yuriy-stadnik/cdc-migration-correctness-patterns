#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic>" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

TOPIC_B64="$(printf '%s' "$1" | base64)"

{
  printf 'TOPIC_B64=%q\n' "${TOPIC_B64}"
  cat <<'REMOTE'
set -euo pipefail
TOPIC="$(printf '%s' "${TOPIC_B64}" | base64 --decode)"
sudo docker exec lab-kafka-1 kafka-topics --bootstrap-server kafka:29092 --describe --topic "${TOPIC}"
REMOTE
} | run_script_on_ec2
