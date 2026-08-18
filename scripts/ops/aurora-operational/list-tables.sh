#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

OPERATIONAL_DB_NAME="${OPERATIONAL_DB_NAME:-operationaldb}"
OP_AURORA_ENDPOINT="$(terraform_output aurora_operational_endpoint)"
OP_SECRET_ARN="$(terraform_output aurora_operational_master_secret_arn)"
require_jq

OP_SECRET_JSON="$(aws secretsmanager get-secret-value --region "${AWS_REGION}" --secret-id "${OP_SECRET_ARN}" --query SecretString --output text)"
OP_DB_PASSWORD_B64="$(printf '%s' "${OP_SECRET_JSON}" | jq -r '.password' | base64)"

{
  printf 'OP_DB_PASSWORD_B64=%q\n' "${OP_DB_PASSWORD_B64}"
  printf 'OP_AURORA_ENDPOINT=%q\n' "${OP_AURORA_ENDPOINT}"
  printf 'OPERATIONAL_DB_NAME=%q\n' "${OPERATIONAL_DB_NAME}"
  cat <<'REMOTE'
set -euo pipefail
OP_DB_PASSWORD="$(printf '%s' "${OP_DB_PASSWORD_B64}" | base64 --decode)"
sudo docker exec -e PGPASSWORD="${OP_DB_PASSWORD}" lab-legacy-postgres-1 \
  psql -h "${OP_AURORA_ENDPOINT}" -U appuser -d "${OPERATIONAL_DB_NAME}" -Atc \
  "SELECT schemaname || '.' || tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1;"
REMOTE
} | run_script_on_ec2
