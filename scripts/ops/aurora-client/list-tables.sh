#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

CLIENT_DB_NAME="${CLIENT_DB_NAME:-clientdb}"
CLIENT_AURORA_ENDPOINT="$(terraform_output aurora_client_endpoint)"
CLIENT_SECRET_ARN="$(terraform_output aurora_client_master_secret_arn)"
require_jq

CLIENT_SECRET_JSON="$(aws secretsmanager get-secret-value --region "${AWS_REGION}" --secret-id "${CLIENT_SECRET_ARN}" --query SecretString --output text)"
CLIENT_DB_PASSWORD_B64="$(printf '%s' "${CLIENT_SECRET_JSON}" | jq -r '.password' | base64)"

{
  printf 'CLIENT_DB_PASSWORD_B64=%q\n' "${CLIENT_DB_PASSWORD_B64}"
  printf 'CLIENT_AURORA_ENDPOINT=%q\n' "${CLIENT_AURORA_ENDPOINT}"
  printf 'CLIENT_DB_NAME=%q\n' "${CLIENT_DB_NAME}"
  cat <<'REMOTE'
set -euo pipefail
CLIENT_DB_PASSWORD="$(printf '%s' "${CLIENT_DB_PASSWORD_B64}" | base64 --decode)"
sudo docker exec -e PGPASSWORD="${CLIENT_DB_PASSWORD}" lab-legacy-postgres-1 \
  psql -h "${CLIENT_AURORA_ENDPOINT}" -U appuser -d "${CLIENT_DB_NAME}" -Atc \
  "SELECT schemaname || '.' || tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1;"
REMOTE
} | run_script_on_ec2
