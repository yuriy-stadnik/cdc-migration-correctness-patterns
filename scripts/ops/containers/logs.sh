#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <container_name> [lines]" >&2
  exit 1
fi

LINES="${2:-200}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

CONTAINER_NAME_B64="$(printf '%s' "$1" | base64)"

{
  printf 'CONTAINER_NAME_B64=%q\n' "${CONTAINER_NAME_B64}"
  printf 'LINES=%q\n' "${LINES}"
  cat <<'REMOTE'
set -euo pipefail
CONTAINER_NAME="$(printf '%s' "${CONTAINER_NAME_B64}" | base64 --decode)"
sudo docker logs --tail "${LINES}" "${CONTAINER_NAME}"
REMOTE
} | run_script_on_ec2
