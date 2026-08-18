#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <sql>" >&2
  exit 1
fi

SQL="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

OPERATIONAL_DB_NAME="${OPERATIONAL_DB_NAME:-operationaldb}"
OP_AURORA_ENDPOINT="$(terraform_output aurora_operational_endpoint)"
OP_SECRET_ARN="$(terraform_output aurora_operational_master_secret_arn)"
require_jq

OP_SECRET_JSON="$(aws secretsmanager get-secret-value --region "${AWS_REGION}" --secret-id "${OP_SECRET_ARN}" --query SecretString --output text)"
OP_DB_PASSWORD_B64="$(printf '%s' "${OP_SECRET_JSON}" | jq -r '.password' | base64)"
SQL_B64="$(printf '%s' "${SQL}" | base64)"

{
  printf 'OP_DB_PASSWORD_B64=%q\n' "${OP_DB_PASSWORD_B64}"
  printf 'OP_AURORA_ENDPOINT=%q\n' "${OP_AURORA_ENDPOINT}"
  printf 'OPERATIONAL_DB_NAME=%q\n' "${OPERATIONAL_DB_NAME}"
  printf 'SQL_B64=%q\n' "${SQL_B64}"
  cat <<'REMOTE'
set -euo pipefail
OP_DB_PASSWORD="$(printf '%s' "${OP_DB_PASSWORD_B64}" | base64 --decode)"
SQL="$(printf '%s' "${SQL_B64}" | base64 --decode)"
sudo docker exec -e PGPASSWORD="${OP_DB_PASSWORD}" lab-legacy-postgres-1 \
  psql -v ON_ERROR_STOP=1 -h "${OP_AURORA_ENDPOINT}" -U appuser -d "${OPERATIONAL_DB_NAME}" -c "${SQL}"
REMOTE
} | run_script_on_ec2
