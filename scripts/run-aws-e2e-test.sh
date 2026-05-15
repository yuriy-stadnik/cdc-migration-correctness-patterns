#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${1:-us-east-1}"
KEY_PATH="${2:-$HOME/.ssh/temp_ec2_key}"
AURORA_WAIT_SECONDS="${AURORA_WAIT_SECONDS:-300}"
CLIENT_DB_NAME="${CLIENT_DB_NAME:-clientdb}"
OPERATIONAL_DB_NAME="${OPERATIONAL_DB_NAME:-operationaldb}"

INSTANCE_ID="$(terraform output -raw ec2_instance_id)"
INSTANCE_PUBLIC_IP="$(terraform output -raw ec2_public_ip)"
MSK_BOOTSTRAP="$(terraform output -raw msk_bootstrap_sasl_iam)"
CLIENT_AURORA_ENDPOINT="$(terraform output -raw aurora_client_endpoint)"
OP_AURORA_ENDPOINT="$(terraform output -raw aurora_operational_endpoint)"
CLIENT_SECRET_ARN="$(terraform output -raw aurora_client_master_secret_arn)"
OP_SECRET_ARN="$(terraform output -raw aurora_operational_master_secret_arn)"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for parsing Secrets Manager JSON." >&2
  exit 1
fi

CLIENT_SECRET_JSON="$(aws secretsmanager get-secret-value \
  --region "${AWS_REGION}" \
  --secret-id "${CLIENT_SECRET_ARN}" \
  --query SecretString \
  --output text)"
OP_SECRET_JSON="$(aws secretsmanager get-secret-value \
  --region "${AWS_REGION}" \
  --secret-id "${OP_SECRET_ARN}" \
  --query SecretString \
  --output text)"

CLIENT_DB_PASSWORD_B64="$(printf '%s' "${CLIENT_SECRET_JSON}" | jq -r '.password' | base64)"
OP_DB_PASSWORD_B64="$(printf '%s' "${OP_SECRET_JSON}" | jq -r '.password' | base64)"

CID="${CID:-$(( $(date +%s) % 1000000000 ))}"
AID="${AID:-$((CID + 100000000))}"
OID="${OID:-$((CID + 200000000))}"
EMAIL="${EMAIL:-aws-e2e-${CID}@example.com}"

echo "Using:"
echo "  instance_id=${INSTANCE_ID}"
echo "  instance_public_ip=${INSTANCE_PUBLIC_IP}"
echo "  customer_id=${CID}"
echo "  account_id=${AID}"
echo "  order_id=${OID}"
echo "  email=${EMAIL}"
echo "  aurora_wait_seconds=${AURORA_WAIT_SECONDS}"

if [[ ! -f "${KEY_PATH}" ]]; then
  echo "SSH private key not found: ${KEY_PATH}" >&2
  exit 1
fi

if [[ ! -f "${KEY_PATH}.pub" ]]; then
  ssh-keygen -y -f "${KEY_PATH}" > "${KEY_PATH}.pub"
fi

aws ec2-instance-connect send-ssh-public-key \
  --region "${AWS_REGION}" \
  --instance-id "${INSTANCE_ID}" \
  --instance-os-user ec2-user \
  --ssh-public-key "file://${KEY_PATH}.pub" >/dev/null

{
  # Send secrets through SSH stdin instead of the SSH command line, so they do
  # not show up in local process listings.
  printf 'CLIENT_DB_PASSWORD_B64=%q\n' "${CLIENT_DB_PASSWORD_B64}"
  printf 'OP_DB_PASSWORD_B64=%q\n' "${OP_DB_PASSWORD_B64}"
  cat <<'REMOTE'
set -euo pipefail

CLIENT_DB_PASSWORD="$(printf '%s' "${CLIENT_DB_PASSWORD_B64}" | base64 --decode)"
OP_DB_PASSWORD="$(printf '%s' "${OP_DB_PASSWORD_B64}" | base64 --decode)"

wait_for_count() {
  local label="$1"
  local expected="$2"
  local sql="$3"
  local db_kind="$4"
  local start now out

  start="$(date +%s)"
  while true; do
    if [[ "${db_kind}" == "client" ]]; then
      out="$(sudo docker exec -e PGPASSWORD="${CLIENT_DB_PASSWORD}" lab-legacy-postgres-1 \
        psql -h "${CLIENT_AURORA_ENDPOINT}" -U appuser -d "${CLIENT_DB_NAME}" -Atc "${sql}" 2>/dev/null || true)"
    else
      out="$(sudo docker exec -e PGPASSWORD="${OP_DB_PASSWORD}" lab-legacy-postgres-1 \
        psql -h "${OP_AURORA_ENDPOINT}" -U appuser -d "${OPERATIONAL_DB_NAME}" -Atc "${sql}" 2>/dev/null || true)"
    fi

    if [[ "${out}" == "${expected}" ]]; then
      echo "${label}: ok (${out})"
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= AURORA_WAIT_SECONDS )); then
      echo "${label}: timeout after ${AURORA_WAIT_SECONDS}s (last=${out}, expected=${expected})" >&2
      echo "SQL: ${sql}" >&2
      return 1
    fi

    sleep 5
  done
}

wait_for_min_count() {
  local label="$1"
  local min_expected="$2"
  local sql="$3"
  local db_kind="$4"
  local start now out

  start="$(date +%s)"
  while true; do
    if [[ "${db_kind}" == "client" ]]; then
      out="$(sudo docker exec -e PGPASSWORD="${CLIENT_DB_PASSWORD}" lab-legacy-postgres-1 \
        psql -h "${CLIENT_AURORA_ENDPOINT}" -U appuser -d "${CLIENT_DB_NAME}" -Atc "${sql}" 2>/dev/null || true)"
    else
      out="$(sudo docker exec -e PGPASSWORD="${OP_DB_PASSWORD}" lab-legacy-postgres-1 \
        psql -h "${OP_AURORA_ENDPOINT}" -U appuser -d "${OPERATIONAL_DB_NAME}" -Atc "${sql}" 2>/dev/null || true)"
    fi

    if [[ "${out}" =~ ^[0-9]+$ ]] && (( out >= min_expected )); then
      echo "${label}: ok (${out} >= ${min_expected})"
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= AURORA_WAIT_SECONDS )); then
      echo "${label}: timeout after ${AURORA_WAIT_SECONDS}s (last=${out}, min_expected=${min_expected})" >&2
      echo "SQL: ${sql}" >&2
      return 1
    fi

    sleep 5
  done
}

echo "Inserting test data into source postgres..."
sudo docker compose -f /opt/lab/docker-compose.yml exec -T legacy-postgres \
  psql -v ON_ERROR_STOP=1 -U postgres -d appdb <<SQL
BEGIN;
INSERT INTO inventory.customers (id, first_name, last_name, email, status, updated_at)
VALUES (${CID}, 'Aws', 'E2E', '${EMAIL}', 'ACTIVE', now())
ON CONFLICT (id) DO UPDATE
SET email = EXCLUDED.email,
    updated_at = now();

INSERT INTO inventory.accounts (account_id, customer_id, account_type, balance, street, city, state, zip_code, home_phone, mobile_phone)
VALUES (${AID}, ${CID}, 'CHECKING', 2500.00, '789 Pine Rd', 'Ridgewood', 'NJ', '07450', '201-555-7777', '201-555-7778')
ON CONFLICT (account_id) DO UPDATE
SET street = EXCLUDED.street,
    home_phone = EXCLUDED.home_phone,
    mobile_phone = EXCLUDED.mobile_phone;

INSERT INTO inventory.orders_flat (order_id, customer_id, product_name, product_category, unit_price, quantity, total_price)
VALUES (${OID}, ${CID}, 'Mechanical Keyboard', 'Electronics', 129.99, 1, 129.99)
ON CONFLICT (order_id) DO UPDATE
SET unit_price = EXCLUDED.unit_price,
    quantity = EXCLUDED.quantity,
    total_price = EXCLUDED.total_price;
COMMIT;
SQL

echo "Checking source Kafka topics..."
sudo docker exec lab-kafka-1 bash -lc \
  "kafka-console-consumer --bootstrap-server kafka:29092 --topic client.customers --from-beginning --timeout-ms 20000 --max-messages 800 2>/dev/null | grep -q '\"id\":${CID}'"
sudo docker exec lab-kafka-1 bash -lc \
  "kafka-console-consumer --bootstrap-server kafka:29092 --topic operational.orders --from-beginning --timeout-ms 20000 --max-messages 800 2>/dev/null | grep -q '\"id\":${OID}'"
sudo docker exec lab-kafka-1 bash -lc \
  "kafka-console-consumer --bootstrap-server kafka:29092 --topic operational.order_items --from-beginning --timeout-ms 20000 --max-messages 800 2>/dev/null | grep -q '\"order_id\":${OID}'"

echo "Checking mirrored MSK topics..."
sudo docker exec lab-mirrormaker2-1 bash -lc \
  "kafka-console-consumer --bootstrap-server ${MSK_BOOTSTRAP} --consumer.config /tmp/client.properties --topic client.customers --from-beginning --timeout-ms 30000 --max-messages 1200 2>/dev/null | grep -q '\"id\":${CID}'"
sudo docker exec lab-mirrormaker2-1 bash -lc \
  "kafka-console-consumer --bootstrap-server ${MSK_BOOTSTRAP} --consumer.config /tmp/client.properties --topic operational.orders --from-beginning --timeout-ms 30000 --max-messages 1200 2>/dev/null | grep -q '\"id\":${OID}'"
sudo docker exec lab-mirrormaker2-1 bash -lc \
  "kafka-console-consumer --bootstrap-server ${MSK_BOOTSTRAP} --consumer.config /tmp/client.properties --topic operational.order_items --from-beginning --timeout-ms 30000 --max-messages 1200 2>/dev/null | grep -q '\"order_id\":${OID}'"

echo "Checking Aurora destination rows..."
wait_for_count "client.customers" "1" "SELECT count(*) FROM client.customers WHERE id = ${CID};" "client"
wait_for_count "client.addresses" "1" "SELECT count(*) FROM client.addresses WHERE customer_id = ${CID};" "client"
wait_for_count "operational.orders" "1" "SELECT count(*) FROM operational.orders WHERE id = ${OID};" "operational"
wait_for_count "operational.order_items" "1" "SELECT count(*) FROM operational.order_items WHERE order_id = ${OID};" "operational"
wait_for_count "operational.contact_numbers" "2" "SELECT count(*) FROM operational.contact_numbers WHERE customer_id = ${CID};" "operational"

echo "Checking source metadata in Aurora..."
wait_for_count "client.customers source metadata" "1" "SELECT count(*) FROM client.customers WHERE id = ${CID} AND source_record_type IS NOT NULL AND source_ts_ms IS NOT NULL AND source_tx_id IS NOT NULL;" "client"
wait_for_count "operational.orders source metadata" "1" "SELECT count(*) FROM operational.orders WHERE id = ${OID} AND source_record_type IS NOT NULL AND source_ts_ms IS NOT NULL AND source_tx_id IS NOT NULL;" "operational"
wait_for_min_count "client tx metadata rows" "2" "SELECT count(*) FROM cdc.transaction_metadata WHERE status IN ('BEGIN','END');" "client"
wait_for_min_count "operational tx metadata rows" "2" "SELECT count(*) FROM cdc.transaction_metadata WHERE status IN ('BEGIN','END');" "operational"

echo "REMOTE_FULL_E2E_OK CID=${CID} OID=${OID} EMAIL=${EMAIL}"
REMOTE
} | ssh -o StrictHostKeyChecking=accept-new -i "${KEY_PATH}" "ec2-user@${INSTANCE_PUBLIC_IP}" \
  "CID='${CID}' AID='${AID}' OID='${OID}' EMAIL='${EMAIL}' MSK_BOOTSTRAP='${MSK_BOOTSTRAP}' \
CLIENT_AURORA_ENDPOINT='${CLIENT_AURORA_ENDPOINT}' OP_AURORA_ENDPOINT='${OP_AURORA_ENDPOINT}' \
CLIENT_DB_NAME='${CLIENT_DB_NAME}' OPERATIONAL_DB_NAME='${OPERATIONAL_DB_NAME}' \
AURORA_WAIT_SECONDS='${AURORA_WAIT_SECONDS}' bash -s"

echo "Done."
