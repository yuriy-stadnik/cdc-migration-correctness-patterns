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

CLIENT_DB_NAME="${CLIENT_DB_NAME:-clientdb}"
CLIENT_AURORA_ENDPOINT="$(terraform_output aurora_client_endpoint)"
CLIENT_SECRET_ARN="$(terraform_output aurora_client_master_secret_arn)"
require_jq

CLIENT_SECRET_JSON="$(aws secretsmanager get-secret-value --region "${AWS_REGION}" --secret-id "${CLIENT_SECRET_ARN}" --query SecretString --output text)"
CLIENT_DB_PASSWORD_B64="$(printf '%s' "${CLIENT_SECRET_JSON}" | jq -r '.password' | base64)"
SQL_B64="$(printf '%s' "${SQL}" | base64)"

{
  printf 'CLIENT_DB_PASSWORD_B64=%q\n' "${CLIENT_DB_PASSWORD_B64}"
  printf 'CLIENT_AURORA_ENDPOINT=%q\n' "${CLIENT_AURORA_ENDPOINT}"
  printf 'CLIENT_DB_NAME=%q\n' "${CLIENT_DB_NAME}"
  printf 'SQL_B64=%q\n' "${SQL_B64}"
  cat <<'REMOTE'
set -euo pipefail
CLIENT_DB_PASSWORD="$(printf '%s' "${CLIENT_DB_PASSWORD_B64}" | base64 --decode)"
SQL="$(printf '%s' "${SQL_B64}" | base64 --decode)"
sudo docker exec -e PGPASSWORD="${CLIENT_DB_PASSWORD}" lab-legacy-postgres-1 \
  psql -v ON_ERROR_STOP=1 -h "${CLIENT_AURORA_ENDPOINT}" -U appuser -d "${CLIENT_DB_NAME}" -c "${SQL}"
REMOTE
} | run_script_on_ec2
