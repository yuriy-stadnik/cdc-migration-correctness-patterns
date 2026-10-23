# Local transaction-preserving CDC stack

The local environment mirrors the production v2 data path with two Compose projects on one Docker network:

```text
normalized PostgreSQL source
  -> Debezium ordered capture
  -> transaction projector
  -> MirrorMaker 2
  -> cloud Kafka emulator
  -> Java apply worker
  -> PostgreSQL destination (client + operational + cdc schemas)
```

The source tables are `inventory.customers`, `addresses`, `contact_numbers`, `products`, `orders`, and `order_items`. Each source row maps directly to one destination row using the same primary key. The source has no projection tables, contribution/conflict arbitration, catalog-deletion API, or global writer lock.

## Start and verify

From the repository root:

```bash
./local/scripts/start-local.sh
./local/tests/run-all-components.sh
./local/scripts/run-local-e2e.sh
```

The end-to-end script writes all six normalized source tables in one transaction and waits for one atomic destination receipt containing seven row mutations.

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

- `cdc.source.capture.v2`
- `cdc.source.baseline.control.v2`
- `cdc.tx.control.v2`
- `cdc.tx.mutations.v2`
- `cdc.tx.baseline.v2`
- `cdc.tx.dlq.v2`

The Debezium connector is named `source-version-v2`; its configuration is in `source/connectors/source-version-v2-config.json`.

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
