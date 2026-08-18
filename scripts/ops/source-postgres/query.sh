#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <sql>" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

SQL_B64="$(printf '%s' "$1" | base64)"

{
  printf 'SQL_B64=%q\n' "${SQL_B64}"
  cat <<'REMOTE'
set -euo pipefail
SQL="$(printf '%s' "${SQL_B64}" | base64 --decode)"
sudo docker exec -i lab-legacy-postgres-1 psql -v ON_ERROR_STOP=1 -U postgres -d appdb -c "${SQL}"
REMOTE
} | run_script_on_ec2
