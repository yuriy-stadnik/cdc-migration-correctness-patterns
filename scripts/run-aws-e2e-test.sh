#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${1:-us-east-1}"
KEY_PATH="${2:-$HOME/.ssh/temp_ec2_key}"
AURORA_WAIT_SECONDS="${AURORA_WAIT_SECONDS:-300}"
DESTINATION_DB_NAME="${DESTINATION_DB_NAME:-destinationdb}"

INSTANCE_ID="$(terraform output -raw ec2_instance_id)"
INSTANCE_PUBLIC_IP="$(terraform output -raw ec2_public_ip)"
MSK_BOOTSTRAP="$(terraform output -raw msk_bootstrap_sasl_iam)"
DESTINATION_ENDPOINT="$(terraform output -raw aurora_endpoint)"
DESTINATION_SECRET_ARN="$(terraform output -raw aurora_master_secret_arn)"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for parsing Secrets Manager JSON." >&2
  exit 1
fi

SECRET_JSON="$(aws secretsmanager get-secret-value \
  --region "${AWS_REGION}" --secret-id "${DESTINATION_SECRET_ARN}" \
  --query SecretString --output text)"
DB_PASSWORD_B64="$(printf '%s' "${SECRET_JSON}" | jq -r '.password' | base64)"

CID="${CID:-$(( $(date +%s) % 1000000000 ))}"
OID="${OID:-$((CID + 200000000))}"
EMAIL="${EMAIL:-aws-e2e-${CID}@example.com}"
PRODUCT="aws-e2e-product-${OID}"

if [[ ! -f "${KEY_PATH}" ]]; then
  echo "SSH private key not found: ${KEY_PATH}" >&2
  exit 1
fi
if [[ ! -f "${KEY_PATH}.pub" ]]; then
  ssh-keygen -y -f "${KEY_PATH}" > "${KEY_PATH}.pub"
fi

aws ec2-instance-connect send-ssh-public-key \
  --region "${AWS_REGION}" --instance-id "${INSTANCE_ID}" --instance-os-user ec2-user \
  --ssh-public-key "file://${KEY_PATH}.pub" >/dev/null

{
  printf 'DB_PASSWORD_B64=%q\n' "${DB_PASSWORD_B64}"
  cat <<'REMOTE'
set -euo pipefail
DB_PASSWORD="$(printf '%s' "${DB_PASSWORD_B64}" | base64 --decode)"

wait_for_count() {
  local label="$1" expected="$2" sql="$3" start now out
  start="$(date +%s)"
  while true; do
    out="$(sudo docker exec -e PGPASSWORD="${DB_PASSWORD}" lab-legacy-postgres-1 \
      psql -h "${DESTINATION_ENDPOINT}" -U appuser -d "${DESTINATION_DB_NAME}" -Atc "${sql}" 2>/dev/null || true)"
    if [[ "${out}" == "${expected}" ]]; then
      echo "${label}: ok (${out})"
      return 0
    fi
    now="$(date +%s)"
    if (( now - start >= AURORA_WAIT_SECONDS )); then
      echo "${label}: timeout (last=${out}, expected=${expected})" >&2
      return 1
    fi
    sleep 5
  done
}

echo "Writing one seven-row normalized source transaction..."
sudo docker compose -f /opt/lab/docker-compose.yml exec -T -e PGPASSWORD=source_app legacy-postgres \
  psql -v ON_ERROR_STOP=1 -U source_app -d appdb <<SQL
BEGIN;
SELECT source_cdc.begin_write();
INSERT INTO inventory.customers(id,first_name,last_name,email,status,updated_at)
VALUES (${CID},'Aws','E2E','${EMAIL}','ACTIVE',now());
INSERT INTO inventory.addresses(customer_id,address_type,street,city,state,zip_code)
VALUES (${CID},'HOME','789 Pine Rd','Ridgewood','NJ','07450');
INSERT INTO inventory.contact_numbers(customer_id,phone_type,phone_number)
VALUES (${CID},'HOME','201-555-7777'),(${CID},'MOBILE','201-555-7778');
INSERT INTO inventory.products(name,category,current_price)
VALUES ('${PRODUCT}','Electronics',129.99);
INSERT INTO inventory.orders(id,customer_id,status)
VALUES (${OID},${CID},'COMPLETED');
INSERT INTO inventory.order_items(order_id,product_name,quantity,price_at_purchase)
VALUES (${OID},'${PRODUCT}',1,129.99);
COMMIT;
SQL

echo "Checking projected transaction topics..."
sudo docker exec lab-kafka-1 bash -lc \
  "kafka-console-consumer --bootstrap-server kafka:29092 --topic cdc.tx.mutations.v2 --from-beginning --timeout-ms 30000 --max-messages 2000 2>/dev/null | grep -q '${PRODUCT}'"
sudo docker exec lab-mirrormaker2-1 bash -lc \
  "kafka-console-consumer --bootstrap-server '${MSK_BOOTSTRAP}' --consumer.config /tmp/client.properties --topic cdc.tx.mutations.v2 --from-beginning --timeout-ms 30000 --max-messages 2000 2>/dev/null | grep -q '${PRODUCT}'"

wait_for_count "client.customers" 1 "SELECT count(*) FROM client.customers WHERE id=${CID}"
wait_for_count "client.addresses" 1 "SELECT count(*) FROM client.addresses WHERE customer_id=${CID}"
wait_for_count "operational.contact_numbers" 2 "SELECT count(*) FROM operational.contact_numbers WHERE customer_id=${CID}"
wait_for_count "operational.products" 1 "SELECT count(*) FROM operational.products WHERE name='${PRODUCT}'"
wait_for_count "operational.orders" 1 "SELECT count(*) FROM operational.orders WHERE id=${OID}"
wait_for_count "operational.order_items" 1 "SELECT count(*) FROM operational.order_items WHERE order_id=${OID}"
wait_for_count "atomic receipt" 1 "SELECT count(*) FROM cdc.v2_receipt r JOIN client.customers c ON c.source_tx_id=r.transaction_id JOIN operational.orders o ON o.source_tx_id=r.transaction_id WHERE c.id=${CID} AND o.id=${OID} AND r.mutation_count=7"

echo "REMOTE_FULL_E2E_OK CID=${CID} OID=${OID} EMAIL=${EMAIL}"
REMOTE
} | ssh -o StrictHostKeyChecking=accept-new -i "${KEY_PATH}" "ec2-user@${INSTANCE_PUBLIC_IP}" \
  "CID='${CID}' OID='${OID}' EMAIL='${EMAIL}' PRODUCT='${PRODUCT}' MSK_BOOTSTRAP='${MSK_BOOTSTRAP}' \
DESTINATION_ENDPOINT='${DESTINATION_ENDPOINT}' DESTINATION_DB_NAME='${DESTINATION_DB_NAME}' \
AURORA_WAIT_SECONDS='${AURORA_WAIT_SECONDS}' bash -s"
