#!/usr/bin/env bash
set -euo pipefail

LAB_DIR="${LAB_DIR:-/opt/lab}"
TOPIC="${TOPIC:-pg1.inventory.customers}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
CUSTOMER_ID="${CUSTOMER_ID:-$(( $(date +%s) % 1000000000 ))}"
FIRST_NAME="${FIRST_NAME:-CdcTest}"
LAST_NAME="${LAST_NAME:-Customer}"
EMAIL="${EMAIL:-cdc-test-${CUSTOMER_ID}@example.com}"
STATUS="${STATUS:-ACTIVE}"

cd "$LAB_DIR"

wait_for_command() {
  local description="$1"
  local timeout="$2"
  shift 2

  local start
  start="$(date +%s)"
  until "$@" >/tmp/ec2-cdc-test-wait.out 2>/tmp/ec2-cdc-test-wait.err; do
    if (( $(date +%s) - start >= timeout )); then
      echo "Timed out waiting for: $description" >&2
      cat /tmp/ec2-cdc-test-wait.out >&2 || true
      cat /tmp/ec2-cdc-test-wait.err >&2 || true
      return 1
    fi
    sleep 3
  done
}

echo "Checking Docker Compose stack in $LAB_DIR..."
wait_for_command "Kafka broker" "$TIMEOUT_SECONDS" \
  docker compose exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list

wait_for_command "legacy PostgreSQL" "$TIMEOUT_SECONDS" \
  docker compose exec -T legacy-postgres pg_isready -U postgres -d appdb

wait_for_command "Kafka Connect REST API" "$TIMEOUT_SECONDS" \
  curl -fsS http://localhost:8083/connectors

echo "Checking Debezium connector status..."
CONNECTOR_STATUS="$(curl -fsS http://localhost:8083/connectors/legacy-postgres-source/status)"
echo "$CONNECTOR_STATUS"
if ! printf '%s' "$CONNECTOR_STATUS" | grep -q '"state":"RUNNING"'; then
  echo "Debezium connector is not RUNNING." >&2
  exit 1
fi

echo "Ensuring Kafka topic exists: $TOPIC"
wait_for_command "Kafka topic $TOPIC" "$TIMEOUT_SECONDS" \
  docker compose exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic "$TOPIC"

echo "Inserting customer id=$CUSTOMER_ID email=$EMAIL into inventory.customers..."
docker compose exec -T legacy-postgres psql -v ON_ERROR_STOP=1 -U postgres -d appdb \
  -c "INSERT INTO inventory.customers (id, first_name, last_name, email, status, updated_at)
      VALUES ($CUSTOMER_ID, '$FIRST_NAME', '$LAST_NAME', '$EMAIL', '$STATUS', now())
      ON CONFLICT (id) DO UPDATE
      SET first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          email = EXCLUDED.email,
          status = EXCLUDED.status,
          updated_at = now();"

echo "Waiting for CDC event in Kafka topic $TOPIC..."
CONSUMER_OUTPUT="/tmp/ec2-cdc-to-kafka-${CUSTOMER_ID}.jsonl"
rm -f "$CONSUMER_OUTPUT"

deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
while (( $(date +%s) < deadline )); do
  docker compose exec -T kafka kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --topic "$TOPIC" \
    --from-beginning \
    --timeout-ms 10000 \
    --property print.key=true \
    --property print.value=true >"$CONSUMER_OUTPUT" 2>/tmp/ec2-cdc-test-consumer.err || true

  if grep -Fq "\"id\":$CUSTOMER_ID" "$CONSUMER_OUTPUT" && grep -Fq "\"email\":\"$EMAIL\"" "$CONSUMER_OUTPUT"; then
    echo "CDC event found for customer id=$CUSTOMER_ID."
    grep -F "\"email\":\"$EMAIL\"" "$CONSUMER_OUTPUT" | tail -n 1
    exit 0
  fi

  sleep 5
done

echo "CDC event was not found in topic $TOPIC within ${TIMEOUT_SECONDS}s." >&2
echo "Last consumer stderr:" >&2
cat /tmp/ec2-cdc-test-consumer.err >&2 || true
echo "Last consumer output tail:" >&2
tail -n 40 "$CONSUMER_OUTPUT" >&2 || true
exit 1
