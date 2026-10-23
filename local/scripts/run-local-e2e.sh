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
BASELINE_DIR=""

cleanup() {
  docker compose -f "$SOURCE_COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose -f "$CLOUD_COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
  docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
}
cleanup_baseline() {
  if [ -n "$BASELINE_DIR" ]; then rm -rf "$BASELINE_DIR"; fi
}
trap cleanup_baseline EXIT
if [ "$RESET" = "1" ]; then cleanup; fi

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

active_baseline="$(docker exec local-cloud-destination-postgres psql -U appuser -d destinationdb -tAc \
  "SELECT count(*) FROM cdc.v2_namespace" | tr -d '[:space:]')"
if [ "$active_baseline" = "0" ]; then
  if [ -n "${JAVA_HOME:-}" ] && [ ! -x "$JAVA_HOME/bin/java" ]; then
    if [ -x /usr/libexec/java_home ]; then
      JAVA_HOME="$(/usr/libexec/java_home -v 17)"
      export JAVA_HOME
    else
      unset JAVA_HOME
    fi
  fi
  BASELINE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/local-v2-baseline.XXXXXX")"
  baseline_bundle="$BASELINE_DIR/baseline.jsonl"
  SOURCE_JDBC_URL="jdbc:postgresql://localhost:5432/appdb" \
  SOURCE_DB_USER="postgres" SOURCE_DB_PASSWORD="postgres" \
  DESTINATION_JDBC_URL="jdbc:postgresql://localhost:5434/destinationdb" \
  DESTINATION_DB_USER="appuser" DESTINATION_DB_PASSWORD="apppass" \
    gradle -p transaction-apply run --no-daemon --args="legacy-postgres local-1 inventory-direct-v2 inventory-direct-v2 POPULATE_EMPTY $baseline_bundle"

  {
    IFS= read -r control
    printf 'baseline-control|%s\n' "$control"
  } < "$baseline_bundle" | docker exec -i local-source-kafka kafka-console-producer \
    --bootstrap-server source-kafka:29092 --topic cdc.source.baseline.control.v2 \
    --property parse.key=true --property key.separator='|'
  tail -n +2 "$baseline_bundle" | awk '{printf "baseline-owner-%08d|%s\n", NR, $0}' | \
    docker exec -i local-source-kafka kafka-console-producer \
      --bootstrap-server source-kafka:29092 --topic cdc.tx.baseline.v2 \
      --property parse.key=true --property key.separator='|'
fi

# One source commit intentionally modifies all six normalized source tables.
docker exec -e PGPASSWORD=source_app -i local-source-postgres psql -v ON_ERROR_STOP=1 -U source_app -d appdb <<SQL
BEGIN;
SELECT source_cdc.begin_write();
INSERT INTO inventory.customers(id,first_name,last_name,email,status,updated_at)
VALUES (${CUSTOMER_ID},'Local','Customer','local-${CUSTOMER_ID}@example.com','ACTIVE',now());
INSERT INTO inventory.addresses(customer_id,address_type,street,city,state,zip_code)
VALUES (${CUSTOMER_ID},'HOME','789 Pine Rd','Ridgewood','NJ','07450');
INSERT INTO inventory.contact_numbers(customer_id,phone_type,phone_number)
VALUES (${CUSTOMER_ID},'HOME','201-555-7777'),(${CUSTOMER_ID},'MOBILE','201-555-7778');
INSERT INTO inventory.products(name,category,current_price)
VALUES ('Local Migration Test Item ${ORDER_ID}','Integration',12.50);
INSERT INTO inventory.orders(id,customer_id,status)
VALUES (${ORDER_ID},${CUSTOMER_ID},'COMPLETED');
INSERT INTO inventory.order_items(order_id,product_name,quantity,price_at_purchase)
VALUES (${ORDER_ID},'Local Migration Test Item ${ORDER_ID}',4,12.50);
COMMIT;
SQL

wait_for_sql "customer" 1 "SELECT count(*) FROM client.customers WHERE id=${CUSTOMER_ID}"
wait_for_sql "address" 1 "SELECT count(*) FROM client.addresses WHERE customer_id=${CUSTOMER_ID}"
wait_for_sql "order" 1 "SELECT count(*) FROM operational.orders WHERE id=${ORDER_ID}"
wait_for_sql "phones" 2 "SELECT count(*) FROM operational.contact_numbers WHERE customer_id=${CUSTOMER_ID}"
wait_for_sql "one atomic applied receipt" 1 "SELECT count(*) FROM cdc.v2_receipt r JOIN client.customers c ON c.source_tx_id=r.transaction_id JOIN operational.orders o ON o.source_tx_id=r.transaction_id WHERE c.id=${CUSTOMER_ID} AND o.id=${ORDER_ID} AND r.mutation_count=7"
wait_for_sql "all seven version effects" 7 "SELECT count(*) FROM cdc.v2_producer v JOIN cdc.v2_receipt r ON r.lane=v.lane AND r.transaction_id=v.producer_id JOIN client.customers c ON c.source_tx_id=r.transaction_id WHERE c.id=${CUSTOMER_ID} AND v.producer_type='TRANSACTION'"

docker exec local-cloud-destination-postgres psql -U appuser -d destinationdb -c \
  "SELECT transaction_id,mutation_set_hash,mutation_count,committed_at FROM cdc.v2_receipt ORDER BY committed_at DESC LIMIT 5"

echo "transaction-preserving local E2E: ok"
if [ "$RESET" = "1" ]; then cleanup; fi
