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
cat > /opt/lab/integration-test-cdc-to-kafka.sh <<'EC2_CDC_TEST_SCRIPT'
${ec2_cdc_test_script}
EC2_CDC_TEST_SCRIPT
chmod +x /opt/lab/integration-test-cdc-to-kafka.sh

# -------------------------------------------------------------------
# Source emulator files: normalized PostgreSQL CDC -> Kafka -> Flink
# -------------------------------------------------------------------
mkdir -p /opt/lab/postgres/init /opt/lab/connectors /opt/lab/config /opt/lab/flink-checkpoints
chmod 0777 /opt/lab/flink-checkpoints
mkdir -p /opt/lab/flink-usrlib
aws s3 cp "s3://${projector_bucket}/${projector_key}" /opt/lab/flink-usrlib/transaction-projector-all.jar

cat > /opt/lab/postgres/init/01-role-and-grants.sql <<'SOURCE_ROLES_SQL'
${source_roles_sql}
SOURCE_ROLES_SQL

cat > /opt/lab/postgres/init/02-inventory.sql <<'SOURCE_INVENTORY_SQL'
${source_inventory_sql}
SOURCE_INVENTORY_SQL

cat > /opt/lab/postgres/init/03-source-version-v2.sql <<'SOURCE_VERSION_SQL'
${source_version_sql}
SOURCE_VERSION_SQL

cat > /opt/lab/connectors/source-version-v2-config.json <<'SOURCE_V2_CONNECTOR'
${source_version_connector_json}
SOURCE_V2_CONNECTOR

cat > /opt/lab/config/transaction-v2-topics.txt <<'TRANSACTION_TOPICS'
${transaction_topics}
TRANSACTION_TOPICS

cat > /opt/lab/config/e2e-source-insert.sql <<'E2E_SOURCE_INSERT_SQL'
${e2e_source_insert_sql}
E2E_SOURCE_INSERT_SQL

cat > /opt/lab/config/e2e-source-update.sql <<'E2E_SOURCE_UPDATE_SQL'
${e2e_source_update_sql}
E2E_SOURCE_UPDATE_SQL

cat > /opt/lab/config/mm2-common.properties <<'MM2_COMMON_PROPERTIES'
${mm2_common_properties}
MM2_COMMON_PROPERTIES

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

# Mirror only required business topics and selected groups
source->target.topics = ${mm2_topic_allowlist}
source->target.groups = ${mm2_group_allowlist}

# Kafka Connect internal topics (on target)
offset.storage.topic = mm2-offsets.target.internal
config.storage.topic = mm2-configs.target.internal
status.storage.topic = mm2-status.target.internal

# Target IAM auth
target.security.protocol = SASL_SSL
target.sasl.mechanism = AWS_MSK_IAM
target.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required;
target.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler

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
      KAFKA_TRANSACTION_MAX_TIMEOUT_MS: "7200000"
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 30

  kafka-topics-init:
    image: confluentinc/cp-kafka:7.7.7
    networks: [cdc]
    depends_on:
      kafka:
        condition: service_healthy
    restart: "no"
    volumes:
      - ./config/transaction-v2-topics.txt:/config/transaction-v2-topics.txt:ro
    entrypoint: ["/bin/bash", "-ec"]
    command: |
      while IFS= read -r spec; do
        [ -n "$$spec" ] || continue
        IFS=: read -r topic partitions policy retention <<<"$$spec"
        kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
          --topic "$$topic" --partitions "$$partitions" --replication-factor 1 \
          --config cleanup.policy="$$policy" --config retention.ms="$$retention"
      done < /config/transaction-v2-topics.txt

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
      - ./config:/config:ro
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
        echo "Running normalized PostgreSQL init SQL..."
        schema_exists=$$(PGPASSWORD=postgres psql -v ON_ERROR_STOP=1 -h legacy-postgres -U postgres -d appdb -Atqc "SELECT to_regclass('source_cdc.schema_version') IS NOT NULL")
        if [ "$$schema_exists" != "t" ]; then
          PGPASSWORD=postgres psql -v ON_ERROR_STOP=1 -h legacy-postgres -U postgres -d appdb -f /init/01-role-and-grants.sql
          PGPASSWORD=postgres psql -v ON_ERROR_STOP=1 -h legacy-postgres -U postgres -d appdb -f /init/02-inventory.sql
          PGPASSWORD=postgres psql -v ON_ERROR_STOP=1 -h legacy-postgres -U postgres -d appdb -f /init/03-source-version-v2.sql
        else
          installed=$$(PGPASSWORD=postgres psql -v ON_ERROR_STOP=1 -h legacy-postgres -U postgres -d appdb -Atqc "SELECT count(*) FROM source_cdc.schema_version WHERE version=4")
          if [ "$$installed" != "1" ]; then
            echo "Incompatible source volume; recreate it to install the normalized source schema." >&2
            exit 1
          fi
          echo "Normalized source schema already installed; skipping seed SQL."
        fi
        echo "pg-init done."

  connect:
    image: debezium/connect:2.7.3.Final
    hostname: connect
    restart: unless-stopped
    networks: [cdc]
    depends_on:
      kafka:
        condition: service_healthy
      kafka-topics-init:
        condition: service_completed_successfully
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
      CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY: "All"
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
          --data @/connectors/source-version-v2-config.json \
          http://connect:8083/connectors/source-version-v2/config)
        cat /tmp/connect-response.txt
        echo
        if [ "$$status" -lt 200 ] || [ "$$status" -ge 300 ]; then
          echo "Connector registration failed with HTTP $$status" >&2
          exit 1
        fi
        curl -sS http://connect:8083/connectors/source-version-v2/status
        echo

  flink-jobmanager:
    image: flink:1.19.1-scala_2.12-java17
    hostname: flink-jobmanager
    restart: unless-stopped
    networks: [cdc]
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      CAPTURE_TOPIC: cdc.source.capture.v2
      SOURCE_DATABASE_ID: legacy-postgres
      SOURCE_INCARNATION: aws-1
      CAPTURE_SCOPE_ID: inventory-direct-v2
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 24
        parallelism.default: 1
        state.backend.type: hashmap
        execution.checkpointing.interval: 10s
        execution.checkpointing.dir: file:///opt/flink/checkpoints
    volumes:
      - ./flink-usrlib:/opt/flink/usrlib:ro
      - ./flink-checkpoints:/opt/flink/checkpoints

  flink-taskmanager:
    image: flink:1.19.1-scala_2.12-java17
    hostname: flink-taskmanager
    restart: unless-stopped
    networks: [cdc]
    depends_on:
      - flink-jobmanager
    command: taskmanager
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      CAPTURE_TOPIC: cdc.source.capture.v2
      SOURCE_DATABASE_ID: legacy-postgres
      SOURCE_INCARNATION: aws-1
      CAPTURE_SCOPE_ID: inventory-direct-v2
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 24
        parallelism.default: 1
        state.backend.type: hashmap
        execution.checkpointing.interval: 10s
        execution.checkpointing.dir: file:///opt/flink/checkpoints
    volumes:
      - ./flink-usrlib:/opt/flink/usrlib:ro
      - ./flink-checkpoints:/opt/flink/checkpoints

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
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      CAPTURE_TOPIC: cdc.source.capture.v2
      SOURCE_DATABASE_ID: legacy-postgres
      SOURCE_INCARNATION: aws-1
      CAPTURE_SCOPE_ID: inventory-direct-v2
    volumes:
      - ./flink-usrlib:/opt/flink/usrlib:ro
    command: >
      bash -lc "
        set -e;
        until (echo > /dev/tcp/flink-jobmanager/8081) >/dev/null 2>&1; do sleep 1; done;
        /opt/flink/bin/flink run -d -m flink-jobmanager:8081 \
          -c com.example.cdc.projector.TransactionProjectorJob \
          /opt/flink/usrlib/transaction-projector-all.jar;
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
  msk-topics-init:
    image: confluentinc/cp-kafka:7.7.7
    restart: "no"
    networks: [cdc]
    volumes:
      - ./config/transaction-v2-topics.txt:/config/transaction-v2-topics.txt:ro
      - ./libs/aws-msk-iam-auth-all.jar:/usr/share/java/kafka/aws-msk-iam-auth-all.jar:ro
      - ./client.properties:/tmp/client.properties:ro
    environment:
      AWS_REGION: "${region}"
      CLASSPATH: "/usr/share/java/kafka/aws-msk-iam-auth-all.jar"
    entrypoint: ["/bin/bash", "-ec"]
    command: |
      while IFS= read -r spec; do
        [ -n "$$spec" ] || continue
        IFS=: read -r topic partitions policy retention <<<"$$spec"
        [[ "$$topic" == cdc.tx.* ]] || continue
        kafka-topics --bootstrap-server "${msk_bootstrap_iam}" --command-config /tmp/client.properties \
          --create --if-not-exists --topic "$$topic" --partitions "$$partitions" \
          --config cleanup.policy="$$policy" --config retention.ms="$$retention"
      done < /config/transaction-v2-topics.txt

  mirrormaker2:
    image: confluentinc/cp-kafka-connect:7.8.0
    restart: unless-stopped
    networks: [cdc]
    depends_on:
      kafka:
        condition: service_healthy
      dbz-init:
        condition: service_completed_successfully
      msk-topics-init:
        condition: service_completed_successfully
    volumes:
      - ./mm2.properties:/etc/kafka/mm2.properties:ro
      - ./config/mm2-common.properties:/etc/kafka/mm2-common.properties:ro
      - ./libs/aws-msk-iam-auth-all.jar:/usr/share/java/kafka/aws-msk-iam-auth-all.jar:ro
      - ./connect-log4j.properties:/usr/config/connect-log4j.properties:ro
      - ./client.properties:/tmp/client.properties:ro
    environment:
      AWS_REGION: "${region}"
      CLASSPATH: "/usr/share/java/kafka/aws-msk-iam-auth-all.jar"
    command: >
      bash -lc "cat /etc/kafka/mm2-common.properties /etc/kafka/mm2.properties > /tmp/mm2.properties && connect-mirror-maker /tmp/mm2.properties"
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
