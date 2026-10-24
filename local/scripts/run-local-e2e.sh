#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

NETWORK_NAME="${LOCAL_DOCKER_NETWORK_NAME:-cdc-migration-local}"
SOURCE_COMPOSE="${SOURCE_COMPOSE:-local/source/docker-compose.yml}"
CLOUD_COMPOSE="${CLOUD_COMPOSE:-local/cloud/docker-compose.yml}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-360}"
ORDER_ID="${ORDER_ID:-$(( $(date +%s) % 1000000000 ))}"
CUSTOMER_ID="${CUSTOMER_ID:-$(( ORDER_ID + 2000 ))}"
RESET="${RESET:-0}"

cleanup() {
  docker compose -f "$SOURCE_COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose -f "$CLOUD_COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
  docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
}
if [ "$RESET" != "1" ]; then
  echo "This lab uses an explicit empty bootstrap. Run with RESET=1 to start from clean volumes." >&2
  exit 2
fi
cleanup

wait_for() {
  local description="$1"; shift
  local start; start="$(date +%s)"
  until "$@" >/tmp/local-e2e-wait.out 2>/tmp/local-e2e-wait.err; do
    if (( $(date +%s) - start >= TIMEOUT_SECONDS )); then
      echo "Timed out waiting for: $description" >&2
      cat /tmp/local-e2e-wait.out /tmp/local-e2e-wait.err >&2 || true
      return 1
    fi
    sleep 5
  done
}

wait_for_sql() {
  local description="$1" expected="$2" sql="$3" start actual
  start="$(date +%s)"
  while true; do
    actual="$(docker exec local-cloud-destination-postgres psql -U appuser -d destinationdb -tAc "$sql" | tr -d '[:space:]')"
    [ "$actual" = "$expected" ] && return 0
    if (( $(date +%s) - start >= TIMEOUT_SECONDS )); then
      echo "Timed out waiting for $description; expected=$expected actual=${actual:-<empty>}" >&2
      return 1
    fi
    sleep 5
  done
}

docker network inspect "$NETWORK_NAME" >/dev/null 2>&1 || docker network create "$NETWORK_NAME" >/dev/null
docker compose -f "$CLOUD_COMPOSE" up -d --build
docker compose -f "$SOURCE_COMPOSE" up -d --build

wait_for "Debezium connector" curl -fsS http://localhost:8083/connectors/source-version-v2/status
wait_for "transaction projector" bash -c "curl -fsS http://localhost:8081/jobs/overview | grep -q transaction-preserving-cdc-projector"
wait_for "destination database" docker exec local-cloud-destination-postgres pg_isready -U appuser -d destinationdb

# The first source commit creates one row in each of the four lab tables.
docker exec -e PGPASSWORD=source_app -i local-source-postgres psql -v ON_ERROR_STOP=1 -U source_app -d appdb <<SQL
BEGIN;
SELECT source_cdc.begin_write();
INSERT INTO inventory.customers(id,first_name,last_name,email,status,updated_at)
VALUES (${CUSTOMER_ID},'Local','Customer','local-${CUSTOMER_ID}@example.com','ACTIVE',now());
INSERT INTO inventory.products(name,category,current_price)
VALUES ('Local Migration Test Item ${ORDER_ID}','Integration',12.50);
INSERT INTO inventory.orders(id,customer_id,status)
VALUES (${ORDER_ID},${CUSTOMER_ID},'COMPLETED');
INSERT INTO inventory.order_items(order_id,product_name,quantity,price_at_purchase)
VALUES (${ORDER_ID},'Local Migration Test Item ${ORDER_ID}',4,12.50);
COMMIT;
SQL

wait_for_sql "customer" 1 "SELECT count(*) FROM client.customers WHERE id=${CUSTOMER_ID}"
wait_for_sql "order" 1 "SELECT count(*) FROM operational.orders WHERE id=${ORDER_ID}"
wait_for_sql "one atomic four-row receipt" 1 "SELECT count(*) FROM cdc.v2_receipt r JOIN client.customers c ON c.source_tx_id=r.transaction_id JOIN operational.orders o ON o.source_tx_id=r.transaction_id WHERE c.id=${CUSTOMER_ID} AND o.id=${ORDER_ID} AND r.mutation_count=4"
wait_for_sql "all four transaction producers" 4 "SELECT count(*) FROM cdc.v2_producer v JOIN cdc.v2_receipt r ON r.lane=v.lane AND r.transaction_id=v.producer_id JOIN client.customers c ON c.source_tx_id=r.transaction_id WHERE c.id=${CUSTOMER_ID} AND v.producer_type='TRANSACTION'"

# The second commit demonstrates exact transaction lineage from version 1 to 2.
docker exec -e PGPASSWORD=source_app -i local-source-postgres psql -v ON_ERROR_STOP=1 -U source_app -d appdb <<SQL
BEGIN;
SELECT source_cdc.begin_write();
UPDATE inventory.customers SET status='VERIFIED',updated_at=now() WHERE id=${CUSTOMER_ID};
UPDATE inventory.orders SET status='SHIPPED' WHERE id=${ORDER_ID};
COMMIT;
SQL

wait_for_sql "customer version two" 1 "SELECT count(*) FROM client.customers WHERE id=${CUSTOMER_ID} AND version=2 AND status='VERIFIED'"
wait_for_sql "order version two" 1 "SELECT count(*) FROM operational.orders WHERE id=${ORDER_ID} AND version=2 AND status='SHIPPED'"
wait_for_sql "second atomic two-row receipt" 1 "SELECT count(*) FROM cdc.v2_receipt r JOIN client.customers c ON c.source_tx_id=r.transaction_id JOIN operational.orders o ON o.source_tx_id=r.transaction_id WHERE c.id=${CUSTOMER_ID} AND o.id=${ORDER_ID} AND r.mutation_count=2"
wait_for_sql "transaction predecessor lineage" 2 "SELECT count(*) FROM cdc.v2_owner_state WHERE producer_type='TRANSACTION' AND version=2"

docker exec local-cloud-destination-postgres psql -U appuser -d destinationdb -c \
  "SELECT transaction_id,mutation_set_hash,mutation_count,committed_at FROM cdc.v2_receipt ORDER BY committed_at DESC LIMIT 5"

echo "transaction-preserving local E2E: ok"
cleanup
