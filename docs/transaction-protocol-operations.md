# Transaction-preserving CDC operations

This is the authoritative operating guide for the normalized source-owned row-version protocol.

## Guarantee and boundary

The system provides at-least-once transport, replay-safe effects, and one atomic destination PostgreSQL transaction for every complete committed source transaction. Kafka delivery may repeat; an immutable receipt makes an identical replay a no-op. A conflicting replay is rejected.

The atomicity boundary is one PostgreSQL/Aurora database containing the `client`, `operational`, and `cdc` schemas. This is not a claim of exactly-once transport or atomicity across databases.

## Active data model

Six normalized source tables map directly to six destination tables using the same primary keys:

| Source | Destination |
|---|---|
| `inventory.customers` | `client.customers` |
| `inventory.addresses` | `client.addresses` |
| `inventory.contact_numbers` | `operational.contact_numbers` |
| `inventory.products` | `operational.products` |
| `inventory.orders` | `operational.orders` |
| `inventory.order_items` | `operational.order_items` |

Every business row has a positive source-managed numeric `version`. Deletes retain lineage in `source_cdc.owner_state`; changes are recorded in the append-only `source_cdc.revision_journal`. A source transaction receives one stable UUID in `source_cdc.transaction_context`.

There are no source projection tables, shared-row contribution arbitration, dirty-owner queue, deferred projection finalizer, catalog-delete command, parent fence owners, or process-wide writer lock.

## Data flow

1. Application transactions call `source_cdc.begin_write()` before changing captured rows.
2. PostgreSQL triggers advance each changed row's version and append its before/after revision to the source journal.
3. Debezium routes the six business tables, transaction context, revision journal, and transaction metadata into the ordered source capture topic.
4. The Flink projector verifies transaction boundaries and journal coverage, then emits one direct mutation for every final changed owner plus foreign-key existence dependencies.
5. MirrorMaker 2 copies the transaction control, mutation, and DLQ topics to the destination Kafka cluster.
6. Lambda or the local worker stages fragments, validates the manifest's `mutationSetHash`, checks exact numeric predecessor versions, and applies the complete mutation set.
7. Business rows, retained owner/producer state, receipt, and outbox records commit together.

## Topics

| Topic | Purpose |
|---|---|
| `cdc.source.capture.v2` | Ordered Debezium input for the projector; source-local only |
| `cdc.source.baseline.control.v2` | Activated baseline control record; source-local only |
| `cdc.tx.baseline.v2` | Baseline owner records consumed by the projector; source-local only |
| `cdc.tx.control.v2` | Immutable transaction manifests |
| `cdc.tx.mutations.v2` | Immutable numbered mutations |
| `cdc.tx.dlq.v2` | Quarantined protocol failures |

Correctness does not depend on control and mutation arrival order. Transaction identity is scoped by source database and incarnation and is derived from the database-issued source UUID, not from a reusable native transaction ID.

## Baseline activation

Capture is activated against a reconciled source/destination baseline. The reconciler supports:

- `POPULATE_EMPTY`: populate a new empty destination from the source and activate its namespace.
- `VERIFY_EXISTING`: require existing destination rows to agree with the source before activation.

For the local stack, `run-local-e2e.sh` creates and publishes the baseline automatically when `cdc.v2_namespace` is empty:

```bash
RESET=1 ./local/scripts/run-local-e2e.sh
```

The generated JSONL bundle contains the baseline control record on line 1 and one baseline-owner record on every remaining line. Publish the control record to `cdc.source.baseline.control.v2` and owner records to `cdc.tx.baseline.v2`. Baseline and namespace evidence is immutable after activation.

## Source write contract

Captured writes must be ordinary PostgreSQL transactions and must establish their transaction context first:

```sql
BEGIN;
SELECT source_cdc.begin_write();
-- INSERT, UPDATE, or DELETE normalized inventory rows
COMMIT;
```

Primary-key updates are rejected. The database owns versions; application code must not assign them. Independent source writers are allowed, while ordinary row/FK locking supplies database concurrency control.

## Operational queries

Destination transaction state:

```sql
SELECT state, count(*)
FROM cdc.v2_transaction
GROUP BY state
ORDER BY state;
```

Waiting dependency graph:

```sql
SELECT projection_generation, predecessor_transaction_id,
       dependent_transaction_id, dependency_type, entity_name,
       required_version, current_version, current_presence
FROM cdc.transaction_dependency_graph
WHERE dependent_state NOT IN ('APPLIED', 'FAILED')
ORDER BY dependent_transaction_id, predecessor_transaction_id;
```

Recent receipts and quarantine evidence:

```sql
SELECT transaction_id, mutation_set_hash, mutation_count, committed_at
FROM cdc.v2_receipt
ORDER BY committed_at DESC;

SELECT incident_id, transaction_id, reason, evidence, opened_at
FROM cdc.v2_quarantine
ORDER BY opened_at DESC;
```

Source lineage:

```sql
SELECT collection, owner_key, version, presence, last_source_transaction_id
FROM source_cdc.owner_state
ORDER BY collection, owner_key;

SELECT source_transaction_id, ordinal, collection, owner_key,
       before_version, after_version, before_presence, after_presence
FROM source_cdc.revision_journal
ORDER BY source_transaction_id, ordinal;
```

## Recovery and retention

- Flink checkpoint state and Kafka positions advance together; the projector sink uses Kafka transactions.
- Re-delivery of an identical complete transaction is idempotent. Reuse of an identity with different canonical mutations fails integrity validation.
- A destination crash before PostgreSQL commit rolls back every business and protocol effect. A crash after commit is recognized from `cdc.v2_receipt`.
- A dependency remains waiting until its exact baseline or transaction producer evidence exists; it is never treated as an inferred absence.
- Source owner/journal lineage and destination baseline, producer, receipt, and outbox evidence have no automatic TTL. Do not delete correctness evidence without a separately proven replay floor and retention procedure.
- Rotate `source_incarnation` after a restore or clone when the prior source lineage cannot be continued safely.

## Verification

```bash
gradle -p transaction-protocol test
gradle -p transaction-projector test
gradle -p transaction-apply test
gradle -p lambda_java test
gradle -p local/cloud/lambda-consumer test
python3 local/tests/test-source-version-v2.py
RESET=1 ./local/scripts/run-local-e2e.sh
terraform fmt -check -recursive
terraform validate
```

The clean local E2E test activates a baseline, changes all six normalized source tables in one commit, and requires one destination receipt containing all seven resulting row mutations.
