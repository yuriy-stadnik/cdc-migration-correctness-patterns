# Local Integration Stack

This directory integrates the local `/Users/yuriy/work/PipeLine` CDC/Flink stack into the current AWS migration project.

It validates the first part of the target architecture before using AWS:

```text
local legacy PostgreSQL
  -> Debezium PostgreSQL CDC connector
  -> local Kafka CDC topic
  -> Flink SQL projection job
  -> local target PostgreSQL
```

## Included From PipeLine

- PostgreSQL source with logical decoding settings.
- One-shot PostgreSQL initializer.
- Debezium PostgreSQL connector registration.
- Kafka and Zookeeper.
- Flink JobManager and TaskManager.
- Flink Kafka, JSON, JDBC, and PostgreSQL connector JARs.
- Kafka UI.
- Flink SQL source table over Debezium CDC topic.

## Ports

```text
5432  legacy PostgreSQL
5433  target PostgreSQL
8080  Kafka UI
8081  Flink UI
8083  Kafka Connect / Debezium REST API
9092  Kafka from host
```

## Start

From the repository root:

```bash
docker compose -f local/docker-compose.yml up -d
```

The stack automatically:

1. Initializes the `inventory.customers` table.
2. Creates the Debezium replication user.
3. Registers the Debezium connector.
4. Submits the Flink SQL projection job.

## Check Status

Kafka Connect connector status:

```bash
curl -fsS http://localhost:8083/connectors/legacy-postgres-source/status
```

Flink jobs:

```bash
curl -fsS http://localhost:8081/jobs
```

Kafka topics:

```bash
docker exec -it local-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

Kafka UI:

```text
http://localhost:8080
```

Flink UI:

```text
http://localhost:8081
```

## Verify CDC Topic

Consume the raw Debezium CDC topic:

```bash
docker exec -it local-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic pg1.inventory.customers \
  --from-beginning \
  --property print.key=true \
  --property print.value=true
```

## Create a Source Change

```bash
docker exec -it local-legacy-postgres psql -U postgres -d appdb \
  -c "INSERT INTO inventory.customers (id, first_name, last_name, email, status) VALUES (3, 'Carol', 'Example', 'carol@example.com', 'ACTIVE') ON CONFLICT (id) DO UPDATE SET updated_at = now();"
```

## Verify Target Projection

```bash
docker exec -it local-target-postgres psql -U appuser -d microservices \
  -c "SELECT * FROM customer_projection ORDER BY id;"
```

## Run Flink SQL Manually

If the one-shot `flink-sql-init` service fails or you change `local/flink/sql/init.sql`, run:

```bash
docker compose -f local/docker-compose.yml run --rm flink-sql-init
```

Interactive SQL client:

```bash
docker exec -it local-flink-jobmanager /opt/flink/bin/sql-client.sh
```

## Logs

```bash
docker compose -f local/docker-compose.yml logs --tail=200 connect
docker compose -f local/docker-compose.yml logs --tail=200 flink-jobmanager
docker compose -f local/docker-compose.yml logs --tail=200 flink-taskmanager
docker compose -f local/docker-compose.yml logs --tail=200 flink-sql-init
```

## Manual Connector Recreate

The compose stack registers the connector automatically. To recreate it manually:

```bash
./local/register-connector.sh
```

If connector registration fails, inspect the one-shot container logs. They now include the Kafka Connect HTTP response body:

```bash
docker compose -f local/docker-compose.yml logs --tail=200 dbz-init
```

## Stop

```bash
docker compose -f local/docker-compose.yml down
```

Delete local volumes:

```bash
docker compose -f local/docker-compose.yml down -v
```

## Current Scope

Implemented locally:

- PostgreSQL source configured for logical replication.
- Seed `inventory.customers` table.
- Debezium replication user.
- Kafka and Zookeeper.
- Debezium Connect.
- Debezium PostgreSQL source connector.
- Flink JobManager and TaskManager.
- Flink SQL CDC source and JDBC target projection.
- Target PostgreSQL with lineage columns.
- Kafka UI.

Not implemented yet:

- Schema registry.
- DLQ topics.
- Automated test assertions.
- MirrorMaker2 to AWS MSK from the local stack.
- Source LSN and transaction ID extraction into target projection.
