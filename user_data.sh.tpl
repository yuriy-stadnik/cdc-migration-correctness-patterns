#!/bin/bash
set -euo pipefail

# Log everything to a file + console (helps debugging)
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

retry() {
  local attempts="$1"
  shift
  local delay=10
  local n=1

  until "$@"; do
    if [ "$n" -ge "$attempts" ]; then
      echo "Command failed after $attempts attempts: $*" >&2
      return 1
    fi
    echo "Command failed, retrying in $delay seconds [$n/$attempts]: $*" >&2
    sleep "$delay"
    n=$((n + 1))
  done
}

# Amazon Linux 2 repo URLs may resolve through dualstack endpoints. For this
# IPv4-only VPC, force yum to IPv4 and give transient repository timeouts room
# to recover.
grep -q '^ip_resolve=4$' /etc/yum.conf || echo 'ip_resolve=4' >> /etc/yum.conf
grep -q '^timeout=30$' /etc/yum.conf || echo 'timeout=30' >> /etc/yum.conf
grep -q '^retries=10$' /etc/yum.conf || echo 'retries=10' >> /etc/yum.conf

retry 6 yum clean metadata
retry 6 yum makecache -y
retry 6 yum update -y
retry 6 amazon-linux-extras install docker -y
systemctl enable --now docker

# Optional tools
retry 6 yum install -y nmap-ncat || true

# Allow ec2-user to run docker without sudo (next login)
usermod -aG docker ec2-user || true

# -------------------------
# Install Docker Compose v2 as a Docker CLI plugin (enables: docker compose ...)
# -------------------------
COMPOSE_VERSION="v2.24.7"
COMPOSE_PLUGIN_DIR="/usr/local/lib/docker/cli-plugins"
COMPOSE_PLUGIN_BIN="$${COMPOSE_PLUGIN_DIR}/docker-compose"

mkdir -p "$${COMPOSE_PLUGIN_DIR}"

retry 6 curl -4 -fL "https://github.com/docker/compose/releases/download/$${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
  -o "$${COMPOSE_PLUGIN_BIN}"
chmod +x "$${COMPOSE_PLUGIN_BIN}"

# Optional: legacy docker-compose command (nice for scripts/tools)
ln -sf "$${COMPOSE_PLUGIN_BIN}" /usr/local/bin/docker-compose

# Verify (must work)
docker compose version
docker-compose version
docker compose version
# -------------------------
# Lab setup
# -------------------------
mkdir -p /opt/lab
cd /opt/lab

mkdir -p libs
curl -fL "https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.9/aws-msk-iam-auth-1.1.9-all.jar" \
  -o libs/aws-msk-iam-auth-all.jar

# Minimal log4j config so Connect/MM2 doesn't die early
cat > connect-log4j.properties <<'EOF'
log4j.rootLogger=INFO, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)%n
EOF

# -------------------------------------------------------------------
# /opt/lab/client.properties (Kafka CLI config for MSK Serverless IAM)
# -------------------------------------------------------------------
cat > /opt/lab/client.properties <<'EOF'
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF
chmod 0644 /opt/lab/client.properties

# -------------------------------------------------------------------
# /opt/lab/integration-test-cdc-to-kafka.sh
# Inserts one customer row and verifies Debezium delivers it to Kafka.
# -------------------------------------------------------------------
cat > /opt/lab/integration-test-cdc-to-kafka.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

LAB_DIR="$${LAB_DIR:-/opt/lab}"
TOPIC="$${TOPIC:-pg1.inventory.customers}"
BOOTSTRAP_SERVER="$${BOOTSTRAP_SERVER:-kafka:29092}"
TIMEOUT_SECONDS="$${TIMEOUT_SECONDS:-120}"
CUSTOMER_ID="$${CUSTOMER_ID:-$(( $(date +%s) % 1000000000 ))}"
FIRST_NAME="$${FIRST_NAME:-CdcTest}"
LAST_NAME="$${LAST_NAME:-Customer}"
EMAIL="$${EMAIL:-cdc-test-$${CUSTOMER_ID}@example.com}"
STATUS="$${STATUS:-ACTIVE}"

cd "$LAB_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not installed on this EC2 instance." >&2
  echo "The EC2 bootstrap did not complete, or this instance was created before the Docker stack was added." >&2
  echo "Check /var/log/user-data.log, or recreate aws_instance.kafka_mm2 so user_data runs again." >&2
  exit 1
fi

if [[ ! -f "$LAB_DIR/docker-compose.yml" ]]; then
  echo "Docker Compose file not found: $LAB_DIR/docker-compose.yml" >&2
  echo "The EC2 bootstrap did not create the local CDC stack. Check /var/log/user-data.log." >&2
  exit 1
fi

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
CONSUMER_OUTPUT="/tmp/ec2-cdc-to-kafka-$${CUSTOMER_ID}.jsonl"
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

echo "CDC event was not found in topic $TOPIC within $${TIMEOUT_SECONDS}s." >&2
echo "Last consumer stderr:" >&2
cat /tmp/ec2-cdc-test-consumer.err >&2 || true
echo "Last consumer output tail:" >&2
tail -n 40 "$CONSUMER_OUTPUT" >&2 || true
exit 1
EOF
chmod +x /opt/lab/integration-test-cdc-to-kafka.sh

# -------------------------------------------------------------------
# On-prem emulator files: legacy PostgreSQL CDC -> Kafka -> Flink
# -------------------------------------------------------------------
mkdir -p /opt/lab/postgres/init /opt/lab/connectors /opt/lab/flink/sql
mkdir -p /opt/lab/flink-usrlib/plugins/kafka

curl -fL "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar" \
  -o /opt/lab/flink-usrlib/flink-connector-jdbc-3.2.0-1.19.jar
curl -fL "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar" \
  -o /opt/lab/flink-usrlib/postgresql-42.7.10.jar
curl -fL "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar" \
  -o /opt/lab/flink-usrlib/plugins/kafka/flink-sql-connector-kafka-3.3.0-1.19.jar
curl -fL "https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.19.1/flink-json-1.19.1.jar" \
  -o /opt/lab/flink-usrlib/plugins/kafka/flink-json-1.19.1.jar

cat > /opt/lab/postgres/init/01-role-and-grants.sql <<'EOF'
SELECT 'CREATE ROLE dbz WITH LOGIN PASSWORD ''dbz'' REPLICATION'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbz')
\gexec

GRANT CONNECT, CREATE ON DATABASE appdb TO dbz;

CREATE SCHEMA IF NOT EXISTS inventory AUTHORIZATION dbz;
EOF

cat > /opt/lab/postgres/init/02-inventory.sql <<'EOF'
CREATE TABLE IF NOT EXISTS inventory.customers (
  id BIGINT PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory.accounts (
  account_id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  account_type TEXT,
  balance DECIMAL(15,2),
  street TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT,
  home_phone TEXT,
  work_phone TEXT,
  mobile_phone TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory.orders_flat (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  order_date TIMESTAMPTZ DEFAULT now(),
  product_name TEXT,
  product_category TEXT,
  unit_price DECIMAL(10,2),
  quantity INTEGER,
  total_price DECIMAL(10,2)
);

ALTER TABLE inventory.customers REPLICA IDENTITY FULL;
ALTER TABLE inventory.accounts REPLICA IDENTITY FULL;
ALTER TABLE inventory.orders_flat REPLICA IDENTITY FULL;

ALTER TABLE inventory.customers OWNER TO dbz;
ALTER TABLE inventory.accounts OWNER TO dbz;
ALTER TABLE inventory.orders_flat OWNER TO dbz;

INSERT INTO inventory.customers (id, first_name, last_name, email, status)
VALUES
  (1, 'Alice', 'Smith', 'alice@example.com', 'ACTIVE'),
  (2, 'Bob', 'Brown', 'bob@example.com', 'ACTIVE')
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory.accounts (
  account_id, customer_id, account_type, balance, street, city, state,
  zip_code, home_phone, work_phone, mobile_phone
)
VALUES
  (101, 1, 'SAVINGS', 5500.00, '123 Maple St', 'Ridgewood', 'NJ', '07450', '201-555-0123', NULL, '201-555-4567'),
  (102, 2, 'CHECKING', 1200.50, '456 Oak Ave', 'Paramus', 'NJ', '07652', '201-555-9999', NULL, NULL)
ON CONFLICT (account_id) DO UPDATE
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
)
VALUES
  (5001, 1, 'Wireless Headphones', 'Electronics', 150.00, 1, 150.00),
  (5002, 1, 'USB-C Cable', 'Electronics', 25.00, 2, 50.00),
  (5003, 2, 'Wireless Headphones', 'Electronics', 150.00, 1, 150.00)
ON CONFLICT (order_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    product_category = EXCLUDED.product_category,
    unit_price = EXCLUDED.unit_price,
    quantity = EXCLUDED.quantity,
    total_price = EXCLUDED.total_price;
EOF

cat > /opt/lab/connectors/legacy-postgres-source-config.json <<'EOF'
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "legacy-postgres",
  "database.port": "5432",
  "database.user": "dbz",
  "database.password": "dbz",
  "database.dbname": "appdb",
  "topic.prefix": "pg1",
  "plugin.name": "pgoutput",
  "schema.include.list": "inventory",
  "table.include.list": "inventory.customers,inventory.accounts,inventory.orders_flat",
  "slot.name": "dbz_slot",
  "publication.autocreate.mode": "filtered",
  "snapshot.mode": "initial",
  "provide.transaction.metadata": "true",
  "tombstones.on.delete": "false",
  "decimal.handling.mode": "string",
  "time.precision.mode": "adaptive_time_microseconds",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}
EOF

cat > /opt/lab/flink/sql/init.sql <<'EOF'
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE customers_cdc (
  `before` ROW<
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    status STRING,
    created_at STRING,
    updated_at STRING
  >,
  `after` ROW<
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    status STRING,
    created_at STRING,
    updated_at STRING
  >,
  op STRING,
  ts_ms BIGINT,
  `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg1.inventory.customers',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE accounts_cdc (
  `before` ROW<
    account_id BIGINT,
    customer_id BIGINT,
    account_type STRING,
    balance DECIMAL(15,2),
    street STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    home_phone STRING,
    work_phone STRING,
    mobile_phone STRING,
    created_at STRING
  >,
  `after` ROW<
    account_id BIGINT,
    customer_id BIGINT,
    account_type STRING,
    balance DECIMAL(15,2),
    street STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    home_phone STRING,
    work_phone STRING,
    mobile_phone STRING,
    created_at STRING
  >,
  op STRING,
  ts_ms BIGINT,
  `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg1.inventory.accounts',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE orders_flat_cdc (
  `before` ROW<
    order_id BIGINT,
    customer_id BIGINT,
    order_date STRING,
    product_name STRING,
    product_category STRING,
    unit_price DECIMAL(10,2),
    quantity INT,
    total_price DECIMAL(10,2)
  >,
  `after` ROW<
    order_id BIGINT,
    customer_id BIGINT,
    order_date STRING,
    product_name STRING,
    product_category STRING,
    unit_price DECIMAL(10,2),
    quantity INT,
    total_price DECIMAL(10,2)
  >,
  op STRING,
  ts_ms BIGINT,
  `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg1.inventory.orders_flat',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE client_customers_topic (
  id BIGINT,
  first_name STRING,
  last_name STRING,
  email STRING,
  status STRING,
  created_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'client.customers',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE client_addresses_topic (
  customer_id BIGINT,
  address_type STRING,
  street STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (customer_id, address_type) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'client.addresses',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_products_topic (
  name STRING,
  category STRING,
  current_price DECIMAL(10,2),
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (name) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.products',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_orders_topic (
  id BIGINT,
  customer_id BIGINT,
  order_date TIMESTAMP(3),
  status STRING,
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.orders',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_order_items_topic (
  order_id BIGINT,
  product_name STRING,
  quantity INT,
  price_at_purchase DECIMAL(10,2),
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (order_id, product_name) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.order_items',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_contact_numbers_topic (
  customer_id BIGINT,
  phone_type STRING,
  phone_number STRING,
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (customer_id, phone_type) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.contact_numbers',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO client_customers_topic
SELECT
  COALESCE(`after`.id, `before`.id) AS id,
  COALESCE(`after`.first_name, `before`.first_name) AS first_name,
  COALESCE(`after`.last_name, `before`.last_name) AS last_name,
  COALESCE(`after`.email, `before`.email) AS email,
  COALESCE(`after`.status, `before`.status) AS status,
  CAST(REPLACE(SUBSTRING(COALESCE(`after`.created_at, `before`.created_at), 1, 19), 'T', ' ') AS TIMESTAMP(3)) AS created_at,
  CAST(REPLACE(SUBSTRING(COALESCE(`after`.updated_at, `before`.updated_at), 1, 19), 'T', ' ') AS TIMESTAMP(3)) AS updated_at,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM customers_cdc;

INSERT INTO client_addresses_topic
SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  'HOME' AS address_type,
  COALESCE(`after`.street, `before`.street) AS street,
  COALESCE(`after`.city, `before`.city) AS city,
  COALESCE(`after`.state, `before`.state) AS state,
  COALESCE(`after`.zip_code, `before`.zip_code) AS zip_code,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM accounts_cdc
WHERE COALESCE(`after`.street, `before`.street) IS NOT NULL;

INSERT INTO operational_contact_numbers_topic
SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  'HOME' AS phone_type,
  COALESCE(`after`.home_phone, `before`.home_phone) AS phone_number,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM accounts_cdc
WHERE COALESCE(`after`.home_phone, `before`.home_phone) IS NOT NULL;

INSERT INTO operational_contact_numbers_topic
SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  'WORK' AS phone_type,
  COALESCE(`after`.work_phone, `before`.work_phone) AS phone_number,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM accounts_cdc
WHERE COALESCE(`after`.work_phone, `before`.work_phone) IS NOT NULL;

INSERT INTO operational_contact_numbers_topic
SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  'MOBILE' AS phone_type,
  COALESCE(`after`.mobile_phone, `before`.mobile_phone) AS phone_number,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM accounts_cdc
WHERE COALESCE(`after`.mobile_phone, `before`.mobile_phone) IS NOT NULL;

INSERT INTO operational_products_topic
SELECT
  COALESCE(`after`.product_name, `before`.product_name) AS name,
  COALESCE(`after`.product_category, `before`.product_category) AS category,
  COALESCE(`after`.unit_price, `before`.unit_price) AS current_price,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM orders_flat_cdc
WHERE COALESCE(`after`.product_name, `before`.product_name) IS NOT NULL;

INSERT INTO operational_orders_topic
SELECT
  COALESCE(`after`.order_id, `before`.order_id) AS id,
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  CAST(REPLACE(SUBSTRING(COALESCE(`after`.order_date, `before`.order_date), 1, 19), 'T', ' ') AS TIMESTAMP(3)) AS order_date,
  'COMPLETED' AS status,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM orders_flat_cdc
WHERE COALESCE(`after`.order_id, `before`.order_id) IS NOT NULL;

INSERT INTO operational_order_items_topic
SELECT
  COALESCE(`after`.order_id, `before`.order_id) AS order_id,
  COALESCE(`after`.product_name, `before`.product_name) AS product_name,
  COALESCE(`after`.quantity, `before`.quantity) AS quantity,
  COALESCE(`after`.unit_price, `before`.unit_price) AS price_at_purchase,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM orders_flat_cdc
WHERE COALESCE(`after`.product_name, `before`.product_name) IS NOT NULL;
EOF

# -------------------------------------------------------------------
# /opt/lab/mm2.properties (Serverless MSK IAM + MM2 lab-safe)
# -------------------------------------------------------------------
if [ -n "${msk_bootstrap_iam}" ]; then
cat > mm2.properties <<EOF
clusters = source, target

# Source (local Docker Kafka)
source.bootstrap.servers = kafka:29092

# Target (MSK Serverless IAM bootstrap)
target.bootstrap.servers = ${msk_bootstrap_iam}

# Enable mirroring
source->target.enabled = true
target->source.enabled = false

# Mirror only required business topics and selected groups
source->target.topics = ${mm2_topic_allowlist}
source->target.groups = ${mm2_group_allowlist}

# Serverless-safe replication defaults
replication.factor = 1
checkpoints.topic.replication.factor = 1
heartbeats.topic.replication.factor = 1
offset-syncs.topic.replication.factor = 1

# Keep internal topics small for a lab
offset-syncs.topic.partitions = 1
checkpoints.topic.partitions = 1
heartbeats.topic.partitions = 1

# Kafka Connect internal topics (on target)
offset.storage.topic = mm2-offsets.target.internal
config.storage.topic = mm2-configs.target.internal
status.storage.topic = mm2-status.target.internal

offset.storage.partitions = 1
config.storage.partitions = 1
status.storage.partitions = 1

offset.storage.replication.factor = 1
config.storage.replication.factor = 1
status.storage.replication.factor = 1

# Target IAM auth
target.security.protocol = SASL_SSL
target.sasl.mechanism = AWS_MSK_IAM
target.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required;
target.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler

# Timeouts
admin.request.timeout.ms = 60000
request.timeout.ms = 60000

replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy
EOF
fi

# -------------------------------------------------------------------
# /opt/lab/docker-compose.yml
# NOTE: I added restart policies so the stack survives transient failures.
# -------------------------------------------------------------------
cat > docker-compose.yml <<'EOF'
networks:
  cdc: {}

volumes:
  legacy-postgres-data: {}
  zookeeper-data: {}
  zookeeper-log: {}
  kafka-data: {}

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.7.7
    hostname: zookeeper
    restart: unless-stopped
    networks: [cdc]
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: "2181"
      ZOOKEEPER_TICK_TIME: "2000"
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-log:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.7.7
    hostname: kafka
    restart: unless-stopped
    networks: [cdc]
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: "1"
      KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 30

  legacy-postgres:
    image: postgres:16-alpine
    hostname: legacy-postgres
    restart: unless-stopped
    networks: [cdc]
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: appdb
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    command:
      - postgres
      - -c
      - wal_level=logical
      - -c
      - max_wal_senders=10
      - -c
      - max_replication_slots=10
      - -c
      - wal_keep_size=256MB
    volumes:
      - legacy-postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d appdb"]
      interval: 5s
      timeout: 3s
      retries: 30

  pg-init:
    image: postgres:16-alpine
    networks: [cdc]
    depends_on:
      legacy-postgres:
        condition: service_healthy
    restart: "no"
    volumes:
      - ./postgres/init:/init:ro
    entrypoint: ["/bin/sh", "-ec"]
    command:
      - |
        set -e
        echo "Running legacy PostgreSQL init SQL..."
        PGPASSWORD=postgres psql -v ON_ERROR_STOP=1 -h legacy-postgres -U postgres -d appdb -f /init/01-role-and-grants.sql
        PGPASSWORD=postgres psql -v ON_ERROR_STOP=1 -h legacy-postgres -U postgres -d appdb -f /init/02-inventory.sql
        echo "pg-init done."

  connect:
    image: debezium/connect:2.7.3.Final
    hostname: connect
    restart: unless-stopped
    networks: [cdc]
    depends_on:
      kafka:
        condition: service_healthy
      pg-init:
        condition: service_completed_successfully
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: "kafka:29092"
      GROUP_ID: "local-connect-cluster"
      CONFIG_STORAGE_TOPIC: "local_connect_configs"
      OFFSET_STORAGE_TOPIC: "local_connect_offsets"
      STATUS_STORAGE_TOPIC: "local_connect_status"
      CONFIG_STORAGE_REPLICATION_FACTOR: "1"
      OFFSET_STORAGE_REPLICATION_FACTOR: "1"
      STATUS_STORAGE_REPLICATION_FACTOR: "1"
      CONNECT_CONFIG_STORAGE_PARTITIONS: "1"
      CONNECT_OFFSET_STORAGE_PARTITIONS: "1"
      CONNECT_STATUS_STORAGE_PARTITIONS: "1"
      KEY_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      KEY_CONVERTER_SCHEMAS_ENABLE: "false"
      VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      REST_HOST_NAME: "0.0.0.0"
      REST_ADVERTISED_HOST_NAME: "connect"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8083/connectors >/dev/null"]
      interval: 10s
      timeout: 5s
      retries: 30

  dbz-init:
    image: curlimages/curl:8.7.1
    networks: [cdc]
    depends_on:
      connect:
        condition: service_healthy
    restart: "no"
    volumes:
      - ./connectors:/connectors:ro
    entrypoint: ["/bin/sh", "-ec"]
    command:
      - |
        echo "Registering/updating Debezium connector..."
        status=$$(curl -sS -o /tmp/connect-response.txt -w "%%{http_code}" \
          -X PUT \
          -H "Content-Type: application/json" \
          --data @/connectors/legacy-postgres-source-config.json \
          http://connect:8083/connectors/legacy-postgres-source/config)
        cat /tmp/connect-response.txt
        echo
        if [ "$$status" -lt 200 ] || [ "$$status" -ge 300 ]; then
          echo "Connector registration failed with HTTP $$status" >&2
          exit 1
        fi
        curl -sS http://connect:8083/connectors/legacy-postgres-source/status
        echo

  flink-jobmanager:
    image: flink:1.19.1-scala_2.12-java17
    hostname: flink-jobmanager
    restart: unless-stopped
    networks: [cdc]
    ports:
      - "8081:8081"
    command: >
      bash -lc "cp -n /opt/flink/usrlib/plugins/kafka/*.jar /opt/flink/lib/ 2>/dev/null || true;
                cp -n /opt/flink/usrlib/*.jar /opt/flink/lib/ 2>/dev/null || true;
                exec /docker-entrypoint.sh jobmanager"
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 24
        parallelism.default: 1
        state.backend.type: hashmap
        execution.checkpointing.interval: 10s
    volumes:
      - ./flink/sql/init.sql:/opt/flink/init.sql:ro
      - ./flink-usrlib/plugins:/opt/flink/plugins:ro
      - ./flink-usrlib:/opt/flink/usrlib:ro

  flink-taskmanager:
    image: flink:1.19.1-scala_2.12-java17
    hostname: flink-taskmanager
    restart: unless-stopped
    networks: [cdc]
    depends_on:
      - flink-jobmanager
    command: >
      bash -lc "cp -n /opt/flink/usrlib/plugins/kafka/*.jar /opt/flink/lib/ 2>/dev/null || true;
                cp -n /opt/flink/usrlib/*.jar /opt/flink/lib/ 2>/dev/null || true;
                exec /docker-entrypoint.sh taskmanager"
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 24
        parallelism.default: 1
        state.backend.type: hashmap
        execution.checkpointing.interval: 10s
    volumes:
      - ./flink-usrlib/plugins:/opt/flink/plugins:ro
      - ./flink-usrlib:/opt/flink/usrlib:ro

  flink-sql-init:
    image: flink:1.19.1-scala_2.12-java17
    networks: [cdc]
    depends_on:
      dbz-init:
        condition: service_completed_successfully
      flink-jobmanager:
        condition: service_started
      flink-taskmanager:
        condition: service_started
    restart: "no"
    volumes:
      - ./flink/sql/init.sql:/opt/flink/init.sql:ro
      - ./flink-usrlib/plugins:/opt/flink/plugins:ro
      - ./flink-usrlib:/opt/flink/usrlib:ro
    command: >
      bash -lc "
        set -e;
        until (echo > /dev/tcp/flink-jobmanager/8081) >/dev/null 2>&1; do sleep 1; done;
        cp -n /opt/flink/usrlib/plugins/kafka/*.jar /opt/flink/lib/ 2>/dev/null || true;
        cp -n /opt/flink/usrlib/*.jar /opt/flink/lib/ 2>/dev/null || true;
        /opt/flink/bin/sql-client.sh -f /opt/flink/init.sql -Drest.address=flink-jobmanager -Drest.port=8081;
      "

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    restart: unless-stopped
    networks: [cdc]
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: "onprem-emulator"
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka:29092"
      KAFKA_CLUSTERS_0_ZOOKEEPER: "zookeeper:2181"

EOF

if [ -n "${msk_bootstrap_iam}" ]; then
cat >> docker-compose.yml <<'EOF'
  mirrormaker2:
    image: confluentinc/cp-kafka-connect:7.8.0
    restart: unless-stopped
    networks: [cdc]
    depends_on:
      kafka:
        condition: service_healthy
      dbz-init:
        condition: service_completed_successfully
    volumes:
      - ./mm2.properties:/etc/kafka/mm2.properties:ro
      - ./libs/aws-msk-iam-auth-all.jar:/usr/share/java/kafka/aws-msk-iam-auth-all.jar:ro
      - ./connect-log4j.properties:/usr/config/connect-log4j.properties:ro
      - ./client.properties:/tmp/client.properties:ro
    environment:
      AWS_REGION: "${region}"
      CLASSPATH: "/usr/share/java/kafka/aws-msk-iam-auth-all.jar"
    command: >
      bash -lc "connect-mirror-maker /etc/kafka/mm2.properties"
EOF
else
  echo "MSK bootstrap not provided; MirrorMaker2 service is disabled for EC2-only testing."
fi

# -------------------------------------------------------------------
# systemd helper: restart the lab after boot any time
# -------------------------------------------------------------------
cat >/usr/local/bin/lab-up.sh <<'EOF'
#!/bin/bash
set -euo pipefail
cd /opt/lab
docker compose up -d
EOF
chmod +x /usr/local/bin/lab-up.sh

cat >/etc/systemd/system/lab-up.service <<'EOF'
[Unit]
Description=Start /opt/lab docker compose stack
After=docker.service network-online.target
Wants=network-online.target
Requires=docker.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/lab-up.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable lab-up.service

# Start now
cd /opt/lab
docker compose up -d

echo "EC2 lab stack started. Kafka on localhost:9092 (host), kafka:29092 (docker net). MM2 replicating to MSK."
echo "Logs: /var/log/user-data.log"
