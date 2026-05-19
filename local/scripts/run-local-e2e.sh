#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$LOCAL_DIR/.." && pwd)"
cd "$REPO_ROOT"

NETWORK_NAME="${LOCAL_DOCKER_NETWORK_NAME:-cdc-migration-local}"
SOURCE_COMPOSE="${SOURCE_COMPOSE:-local/source/docker-compose.yml}"
CLOUD_COMPOSE="${CLOUD_COMPOSE:-local/cloud/docker-compose.yml}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-240}"
ORDER_ID="${ORDER_ID:-$(( $(date +%s) % 1000000000 ))}"
ACCOUNT_ID="${ACCOUNT_ID:-$(( ORDER_ID + 1000 ))}"
CUSTOMER_ID="${CUSTOMER_ID:-$(( ORDER_ID + 2000 ))}"
RESET="${RESET:-0}"

cleanup() {
  docker compose -f "$SOURCE_COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose -f "$CLOUD_COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
  docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
}

if [ "$RESET" = "1" ]; then
  cleanup
fi

wait_for() {
  local description="$1"
  shift

  local start
  start="$(date +%s)"
  until "$@" >/tmp/local-e2e-wait.out 2>/tmp/local-e2e-wait.err; do
    if (( $(date +%s) - start >= TIMEOUT_SECONDS )); then
      echo "Timed out waiting for: $description" >&2
      cat /tmp/local-e2e-wait.out >&2 || true
      cat /tmp/local-e2e-wait.err >&2 || true
      return 1
    fi
    sleep 5
  done
}

wait_for_sql_count() {
  local service="$1"
  local database="$2"
  local description="$3"
  local expected="$4"
  local sql="$5"
  local actual

  local start
  start="$(date +%s)"
  while true; do
    actual="$(docker exec -i "$service" psql -U appuser -d "$database" -tAc "$sql" | tr -d '[:space:]')"
    if [ "$actual" = "$expected" ]; then
      return 0
    fi
    if (( $(date +%s) - start >= TIMEOUT_SECONDS )); then
      echo "Timed out waiting for: $description" >&2
      echo "Expected: $expected" >&2
      echo "Actual:   ${actual:-<empty>}" >&2
      return 1
    fi
    sleep 5
  done
}

docker network inspect "$NETWORK_NAME" >/dev/null 2>&1 || docker network create "$NETWORK_NAME" >/dev/null

docker compose -f "$CLOUD_COMPOSE" up -d --build
docker compose -f "$SOURCE_COMPOSE" up -d

wait_for "source Kafka" docker exec local-source-kafka kafka-topics --bootstrap-server source-kafka:29092 --list
wait_for "source Connect" curl -fsS http://localhost:8083/connectors/legacy-postgres-source/status
wait_for "source Flink" curl -fsS http://localhost:8081/jobs
wait_for "cloud Kafka" docker exec local-cloud-kafka kafka-topics --bootstrap-server cloud-kafka:39092 --list
wait_for "cloud client Postgres" docker exec local-cloud-client-postgres pg_isready -U appuser -d clientdb
wait_for "cloud operational Postgres" docker exec local-cloud-operational-postgres pg_isready -U appuser -d operationaldb
wait_for "local Lambda replacement" docker logs local-cloud-lambda --tail 20

echo "Inserting source rows..."
docker exec -i local-source-postgres psql -v ON_ERROR_STOP=1 -U postgres -d appdb <<SQL
INSERT INTO inventory.customers (
  id, first_name, last_name, email, status, updated_at
) VALUES (
  ${CUSTOMER_ID}, 'Local', 'Customer', 'local-${CUSTOMER_ID}@example.com', 'ACTIVE', now()
) ON CONFLICT (id) DO UPDATE
SET first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    status = EXCLUDED.status,
    updated_at = now();

INSERT INTO inventory.accounts (
  account_id, customer_id, account_type, balance, street, city, state,
  zip_code, home_phone, work_phone, mobile_phone
) VALUES (
  ${ACCOUNT_ID}, ${CUSTOMER_ID}, 'CHECKING', 777.77, '789 Pine Rd', 'Ridgewood', 'NJ',
  '07450', '201-555-7777', NULL, '201-555-7778'
) ON CONFLICT (account_id) DO UPDATE
SET account_type = EXCLUDED.account_type,
    balance = EXCLUDED.balance,
    street = EXCLUDED.street,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    zip_code = EXCLUDED.zip_code,
    home_phone = EXCLUDED.home_phone,
    work_phone = EXCLUDED.work_phone,
    mobile_phone = EXCLUDED.mobile_phone;

INSERT INTO inventory.orders_flat (
  order_id, customer_id, product_name, product_category, unit_price,
  quantity, total_price
) VALUES (
  ${ORDER_ID}, ${CUSTOMER_ID}, 'Local Migration Test Item', 'Integration', 12.50, 4, 50.00
) ON CONFLICT (order_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    product_category = EXCLUDED.product_category,
    unit_price = EXCLUDED.unit_price,
    quantity = EXCLUDED.quantity,
    total_price = EXCLUDED.total_price;
SQL

echo "Waiting for replication into cloud replacement Postgres..."
wait_for "source operational order topic" docker exec local-source-kafka sh -c "kafka-console-consumer --bootstrap-server source-kafka:29092 --topic operational.orders --from-beginning --timeout-ms 5000 --max-messages 20 2>/dev/null | grep -q '\"id\":${ORDER_ID}'"
wait_for "source client customer topic" docker exec local-source-kafka sh -c "kafka-console-consumer --bootstrap-server source-kafka:29092 --topic client.customers --from-beginning --timeout-ms 5000 --max-messages 20 2>/dev/null | grep -q '\"id\":${CUSTOMER_ID}'"
wait_for "cloud transaction metadata topic" docker exec local-cloud-kafka sh -c "kafka-console-consumer --bootstrap-server cloud-kafka:39092 --topic pg1.transaction --from-beginning --timeout-ms 5000 --max-messages 20 2>/dev/null | grep -q '\"status\":\"END\"'"
wait_for_sql_count local-cloud-client-postgres clientdb "cloud client customer row" "1" "SELECT count(*) FROM client.customers WHERE id = ${CUSTOMER_ID}"
wait_for_sql_count local-cloud-client-postgres clientdb "cloud client address row" "1" "SELECT count(*) FROM client.addresses WHERE customer_id = ${CUSTOMER_ID} AND street = '789 Pine Rd'"
wait_for_sql_count local-cloud-operational-postgres operationaldb "cloud operational order row" "1" "SELECT count(*) FROM operational.orders WHERE id = ${ORDER_ID}"
wait_for_sql_count local-cloud-operational-postgres operationaldb "cloud operational home phone" "1" "SELECT count(*) FROM operational.contact_numbers WHERE customer_id = ${CUSTOMER_ID} AND phone_type = 'HOME' AND phone_number = '201-555-7777'"
wait_for_sql_count local-cloud-operational-postgres operationaldb "cloud operational mobile phone" "1" "SELECT count(*) FROM operational.contact_numbers WHERE customer_id = ${CUSTOMER_ID} AND phone_type = 'MOBILE' AND phone_number = '201-555-7778'"
wait_for_sql_count local-cloud-client-postgres clientdb "cloud client source metadata" "1" "SELECT count(*) FROM client.customers WHERE id = ${CUSTOMER_ID} AND source_record_type = 'I' AND source_ts_ms IS NOT NULL AND source_tx_id IS NOT NULL"
wait_for_sql_count local-cloud-operational-postgres operationaldb "cloud operational source metadata" "1" "SELECT count(*) FROM operational.orders WHERE id = ${ORDER_ID} AND source_record_type = 'I' AND source_ts_ms IS NOT NULL AND source_tx_id IS NOT NULL"
wait_for_sql_count local-cloud-operational-postgres operationaldb "cloud transaction metadata end row" "1" "SELECT CASE WHEN count(*) >= 1 THEN 1 ELSE 0 END FROM cdc.transaction_metadata WHERE status = 'END' AND event_count IS NOT NULL"

echo "Cloud client counts:"
docker exec -i local-cloud-client-postgres psql -U appuser -d clientdb <<'SQL'
SELECT 'customers' AS table_name, count(*) FROM client.customers
UNION ALL SELECT 'addresses', count(*) FROM client.addresses
UNION ALL SELECT 'transaction_metadata', count(*) FROM cdc.transaction_metadata
ORDER BY table_name;
SQL

echo "Cloud operational counts:"
docker exec -i local-cloud-operational-postgres psql -U appuser -d operationaldb <<'SQL'
SELECT 'products' AS table_name, count(*) FROM operational.products
UNION ALL SELECT 'orders', count(*) FROM operational.orders
UNION ALL SELECT 'order_items', count(*) FROM operational.order_items
UNION ALL SELECT 'contact_numbers', count(*) FROM operational.contact_numbers
UNION ALL SELECT 'transaction_metadata', count(*) FROM cdc.transaction_metadata
ORDER BY table_name;
SQL

echo "Cloud client sample:"
docker exec -i local-cloud-client-postgres psql -U appuser -d clientdb -c \
  "SELECT c.id AS customer_id, c.first_name, c.last_name, a.street, a.city, c.source_record_type, c.source_tx_id
   FROM client.customers c
   JOIN client.addresses a ON a.customer_id = c.id
   WHERE c.id = ${CUSTOMER_ID};"

echo "Cloud order sample:"
docker exec -i local-cloud-operational-postgres psql -U appuser -d operationaldb -c \
  "SELECT o.id AS order_id, o.customer_id, p.name AS product, oi.quantity, oi.price_at_purchase, o.source_record_type, o.source_tx_id
   FROM operational.orders o
   JOIN operational.order_items oi ON oi.order_id = o.id
   JOIN operational.products p ON p.name = oi.product_name
   WHERE o.id = ${ORDER_ID};"

echo "Cloud transaction metadata sample:"
docker exec -i local-cloud-operational-postgres psql -U appuser -d operationaldb -c \
  "SELECT tx_id, status, event_count, ts_ms
   FROM cdc.transaction_metadata
   WHERE status = 'END'
   ORDER BY updated_at DESC
   LIMIT 5;"

echo "Cloud contact sample:"
docker exec -i local-cloud-operational-postgres psql -U appuser -d operationaldb -c \
  "SELECT customer_id, phone_type, phone_number
   FROM operational.contact_numbers
   WHERE customer_id = ${CUSTOMER_ID}
   ORDER BY phone_type;"

if [ "$RESET" = "1" ]; then
  cleanup
fi
