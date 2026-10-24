# Local transaction protocol operations

This is the runbook for the simplified local lab. For a concrete row-by-row trace, see [transaction-working-example.md](transaction-working-example.md).

## Contract

The source owns four normalized tables and splits them into two destination schemas:

| Source | Destination |
|---|---|
| `inventory.customers` | `client.customers` |
| `inventory.products` | `operational.products` |
| `inventory.orders` | `operational.orders` |
| `inventory.order_items` | `operational.order_items` |

The source database retains `source_cdc.owner_state` and append-only `source_cdc.revision_journal`. Debezium sends business rows, journal rows, context rows, and transaction boundaries through `cdc.source.capture.v2`. The Java/Flink projector emits:

- `cdc.tx.mutations.v2`: numbered destination mutations;
- `cdc.tx.control.v2`: the signed manifest and complete mutation-set hash;
- `cdc.tx.dlq.v2`: invalid capture diagnostics.

The destination stages fragments, verifies their hash, exact predecessor producer, versions, graph, and foreign-key presence, then applies both destination schemas and the receipt in one PostgreSQL transaction.

## Empty bootstrap

The local lab deliberately starts with empty source and destination volumes. The fixed producer reference `BASELINE/empty-bootstrap-v1` may prove only `ABSENT` for the first revision of an owner. It is not a snapshot and does not support adopting pre-existing rows. Later revisions must name the exact `TRANSACTION` that produced the prior version.

This simplification removes dynamic baseline records/topics/reconciliation. Always run the E2E test with clean volumes:

```bash
RESET=1 ./local/scripts/run-local-e2e.sh
```

## Preserved correctness mechanisms

- `ProducerType` and exact producer lineage remain part of the wire contract.
- `cdc.v2_dependency` and projector state retain the dependency graph and cycle detection.
- Separate control and mutation topics remain, even though each has one local partition.
- `source_cdc.owner_state` and `source_cdc.revision_journal` remain the authoritative source lifecycle evidence.
- `cdc.v2_owner_state`, `cdc.v2_producer`, and `cdc.v2_receipt` retain applied lineage and atomic completion evidence.
- A single local apply worker replaces global advisory/key locks; transaction-row locking serializes local work.

Persistent baseline, namespace, logical-lock, outbox, and quarantine tables are intentionally absent. Invalid source records still go to the DLQ; failed destination transactions remain visible as `FAILED` with `last_error`.

## Verification

```bash
python3 local/tests/test-source-version-v2.py
gradle -p transaction-protocol test
gradle -p transaction-projector test
gradle -p transaction-apply test
docker compose -f local/source/docker-compose.yml config -q
docker compose -f local/cloud/docker-compose.yml config -q
RESET=1 ./local/scripts/run-local-e2e.sh
```

Inspect recent atomic receipts:

```bash
docker exec local-cloud-destination-postgres psql -U appuser -d destinationdb -c "SELECT transaction_id,mutation_set_hash,mutation_count,committed_at FROM cdc.v2_receipt ORDER BY committed_at DESC LIMIT 5"
```

Inspect lineage and graph state:

```sql
SELECT owner->>'collection',version,presence,producer_type,producer_id
FROM cdc.v2_owner_state ORDER BY owner_key;

SELECT * FROM cdc.transaction_dependency_graph
ORDER BY dependent_transaction_id,owner_key;
```

## AWS boundary

AWS wiring now consumes the same source SQL, connector JSON, topic specification, MM2 defaults, E2E transaction SQL, projector, protocol, and apply modules as local deployment. Lambda reserved concurrency is one so the AWS adapter preserves the simplified single-writer assumption.

Build both deployable JARs with `./scripts/build-deployment-artifacts.sh`. Existing source volumes at schema version 3 and destination databases with the previous migration checksum are intentionally rejected; use clean lab resources for this contract.

MSK activation is two-phase: deploy with `enable_msk=true` and mappings disabled, wait for the EC2 `msk-topics-init` service to create the three shared transaction topics, then set `enable_msk_event_source_mappings=true`. This follows AWS's requirement that the Kafka topic exist before its Lambda event-source mapping.

Only local/offline verification was performed. No AWS API, deployment, or AWS E2E test was run, so an isolated AWS rehearsal is still required before declaring AWS readiness.
