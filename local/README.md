# Local transaction-preserving CDC stack

The local environment mirrors the production v2 data path with two Compose projects on one Docker network:

```text
normalized PostgreSQL source
  -> Debezium / Kafka Connect
  -> source Kafka: cdc.source.capture.v2
  -> Flink transaction projector
  -> source Kafka: cdc.tx.mutations.v2 + cdc.tx.control.v2 + cdc.tx.dlq.v2
  -> MirrorMaker 2
  -> cloud Kafka emulator: same cdc.tx.* topics
  -> Java apply worker
  -> PostgreSQL destination (client + operational + cdc schemas)
```

The source tables are `inventory.customers`, `products`, `orders`, and `order_items`. They split into `client.customers` and the three `operational.*` tables while one destination transaction preserves the source commit boundary. The source has no projection tables, contribution/conflict arbitration, catalog-deletion API, snapshot reconciliation, or global writer lock.

## Flink transaction projector

Debezium has `provide.transaction.metadata=true`. It emits transaction `BEGIN`/`END` metadata on its transaction topic and row events on its table topics. The connector's `RegexRouter` combines those records into the single-partition source Kafka topic `cdc.source.capture.v2`. Changes from `source_cdc.transaction_context` and `source_cdc.revision_journal` are application-level evidence; they do not generate the Debezium `BEGIN`/`END` records.

The Flink job consumes `cdc.source.capture.v2` from `source-kafka:29092` and:

1. Buffers immutable `BEGIN`, row-change, revision-evidence, and `END` records in Flink checkpoint state until the source transaction can be assembled.
2. Verifies source versions and reduces multiple changes of the same row to one net transition.
3. Creates `WRITE_PREDECESSOR` dependencies for row-version ordering and `EXISTS` dependencies for foreign-key parents.
4. Selects the destination table in each mutation:
   - `inventory.customers` -> `client.customers`
   - `inventory.products` -> `operational.products`
   - `inventory.orders` -> `operational.orders`
   - `inventory.order_items` -> `operational.order_items`
5. Publishes signed mutations to `cdc.tx.mutations.v2`, one transaction manifest to `cdc.tx.control.v2`, or rejected evidence to `cdc.tx.dlq.v2` on the source Kafka broker.

Flink does not connect to destination PostgreSQL. MirrorMaker 2 copies the three `cdc.tx.*.v2` topics to `cloud-kafka:39092`; the Java apply worker consumes them, stages protocol state in the destination `cdc.v2_*` tables, and atomically applies business rows to the `client` and `operational` schemas.

## Start and verify

From the repository root:

```bash
./local/scripts/start-local.sh
./local/tests/run-all-components.sh
RESET=1 ./local/scripts/run-local-e2e.sh
```

The end-to-end script starts from clean volumes, writes all four normalized source tables in one transaction, and waits for one atomic destination receipt containing four row mutations. It then updates the customer and order together and verifies their shared two-mutation receipt and transaction producer lineage.

To rebuild with clean volumes:

```bash
RESET=1 ./local/scripts/run-local-e2e.sh
```

To stop the deployment:

```bash
./local/scripts/stop-local.sh
DELETE_VOLUMES=1 ./local/scripts/stop-local.sh
```

## Topics

Both local Compose and AWS EC2 bootstrap read the topic definitions from `config/transaction-v2-topics.txt`. Local and AWS MirrorMaker configurations also share `config/mm2-common.properties`.

- `cdc.source.capture.v2`
- `cdc.tx.control.v2`
- `cdc.tx.mutations.v2`
- `cdc.tx.dlq.v2`

The Debezium connector is named `source-version-v2`; its configuration is in `source/connectors/source-version-v2-config.json`.

The local and AWS E2E scripts execute the same source SQL from `config/e2e-source-insert.sql` and `config/e2e-source-update.sql`.

## Focused tests

The source trigger and journal contract can be tested without starting the full stack:

```bash
python3 local/tests/test-source-version-v2.py
```

The Java modules use their own Gradle builds:

```bash
(cd transaction-protocol && gradle test)
(cd transaction-projector && gradle test)
(cd transaction-apply && gradle test)
```
