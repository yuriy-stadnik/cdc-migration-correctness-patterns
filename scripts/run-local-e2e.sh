#!/usr/bin/env bash
set -euo pipefail

NETWORK_NAME="${LOCAL_DOCKER_NETWORK_NAME:-cdc-migration-local}"
SOURCE_COMPOSE="${SOURCE_COMPOSE:-local/source/docker-compose.yml}"
CLOUD_COMPOSE="${CLOUD_COMPOSE:-local/cloud/docker-compose.yml}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-240}"
ORDER_ID="${ORDER_ID:-$(( $(date +%s) % 1000000000 ))}"
ACCOUNT_ID="${ACCOUNT_ID:-$(( ORDER_ID + 1000 ))}"
CUSTOMER_ID="${CUSTOMER_ID:-1}"
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
  local description="$1"
  local expected="$2"
  local sql="$3"
  local actual

  local start
  start="$(date +%s)"
  while true; do
    actual="$(docker exec -i local-cloud-postgres psql -U appuser -d appdb -tAc "$sql" | tr -d '[:space:]')"
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
wait_for "cloud Postgres" docker exec local-cloud-postgres pg_isready -U appuser -d appdb
wait_for "local Lambda replacement" docker logs local-cloud-lambda --tail 20

echo "Inserting source rows..."
docker exec -i local-source-postgres psql -v ON_ERROR_STOP=1 -U postgres -d appdb <<SQL
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
  ${ORDER_ID}, 1, 'Local Migration Test Item', 'Integration', 12.50, 4, 50.00
) ON CONFLICT (order_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    product_category = EXCLUDED.product_category,
    unit_price = EXCLUDED.unit_price,
    quantity = EXCLUDED.quantity,
    total_price = EXCLUDED.total_price;
SQL

echo "Waiting for replication into cloud replacement Postgres..."
wait_for "source operational order topic" docker exec local-source-kafka sh -c "kafka-console-consumer --bootstrap-server source-kafka:29092 --topic operational.orders --from-beginning --timeout-ms 5000 --max-messages 20 2>/dev/null | grep -q '\"id\":${ORDER_ID}'"
wait_for_sql_count "cloud order row" "1" "SELECT count(*) FROM operational.orders WHERE id = ${ORDER_ID}"
wait_for_sql_count "cloud address row" "1" "SELECT count(*) FROM operational.addresses WHERE customer_id = ${CUSTOMER_ID} AND street = '789 Pine Rd'"
wait_for_sql_count "cloud home phone" "1" "SELECT count(*) FROM operational.contact_numbers WHERE customer_id = ${CUSTOMER_ID} AND phone_type = 'HOME' AND phone_number = '201-555-7777'"
wait_for_sql_count "cloud mobile phone" "1" "SELECT count(*) FROM operational.contact_numbers WHERE customer_id = ${CUSTOMER_ID} AND phone_type = 'MOBILE' AND phone_number = '201-555-7778'"

echo "Cloud counts:"
docker exec -i local-cloud-postgres psql -U appuser -d appdb <<'SQL'
SELECT 'products' AS table_name, count(*) FROM operational.products
UNION ALL SELECT 'orders', count(*) FROM operational.orders
UNION ALL SELECT 'order_items', count(*) FROM operational.order_items
UNION ALL SELECT 'addresses', count(*) FROM operational.addresses
UNION ALL SELECT 'contact_numbers', count(*) FROM operational.contact_numbers
ORDER BY table_name;
SQL

echo "Cloud order sample:"
docker exec -i local-cloud-postgres psql -U appuser -d appdb -c \
  "SELECT o.id AS order_id, o.customer_id, p.name AS product, oi.quantity, oi.price_at_purchase
   FROM operational.orders o
   JOIN operational.order_items oi ON oi.order_id = o.id
   JOIN operational.products p ON p.name = oi.product_name
   WHERE o.id = ${ORDER_ID};"

echo "Cloud contact sample:"
docker exec -i local-cloud-postgres psql -U appuser -d appdb -c \
  "SELECT a.customer_id, a.street, a.city, c.phone_type, c.phone_number
   FROM operational.addresses a
   JOIN operational.contact_numbers c ON c.customer_id = a.customer_id
   WHERE a.customer_id = ${CUSTOMER_ID}
   ORDER BY c.phone_type;"

if [ "$RESET" = "1" ]; then
  cleanup
fi
