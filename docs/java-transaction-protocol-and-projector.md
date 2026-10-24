# Java transaction protocol and projector

## Modules

- `transaction-protocol` defines the only v2 wire records, canonical JSON, identifiers, hashes, and validation.
- `transaction-projector` reconstructs committed source transactions from ordered Debezium evidence and plans immutable manifests/mutations.
- `transaction-apply` stages those records and atomically applies one complete source transaction to the destination database.

## Data path

```text
PostgreSQL transaction
  -> source_cdc context + revision journal
  -> Debezium cdc.source.capture.v2
  -> V2Projector
  -> cdc.tx.control.v2 + cdc.tx.mutations.v2
  -> MirrorMaker2
  -> TransactionApplyService
  -> one PostgreSQL commit across client + operational + cdc
```

The projector entry point is [`TransactionProjectorJob.java`](../transaction-projector/src/main/java/com/example/cdc/projector/TransactionProjectorJob.java). It consumes one ordered capture partition with `read_committed` isolation. [`V2Capture.java`](../transaction-projector/src/main/java/com/example/cdc/projector/V2Capture.java) validates BEGIN/END evidence, source UUID, native XID/LSN, collection counts, journal continuity, and row images. [`V2Projector.java`](../transaction-projector/src/main/java/com/example/cdc/projector/V2Projector.java) resolves predecessors, builds the dependency graph, detects cycles, and signs the output.

## Lineage

`ProducerType` remains `BASELINE` or `TRANSACTION`:

- `BASELINE/empty-bootstrap-v1` is a fixed local marker that can prove only an owner's initial `ABSENT` state on clean volumes.
- `TRANSACTION/<transaction-id>` names the exact earlier committed transaction that produced a required version.

There are no dynamic baseline records or baseline input topics. The retained `baselineId` field is fixed to `empty-bootstrap-v1`, keeping the wire model explicit without a reconciliation subsystem.

## Dependencies

- `WRITE_PREDECESSOR` links every transition to its exact prior producer.
- `EXISTS` proves a required foreign-key parent version.

The projector stores transaction edges and rejects graph cycles. The destination persists dependencies in `cdc.v2_dependency`, verifies transaction receipts and producer rows, and leaves unresolved predecessors in `WAITING`.

## Atomic apply

[`V2TransactionApplyService.java`](../transaction-apply/src/main/java/com/example/cdc/apply/V2TransactionApplyService.java) verifies the manifest/mutation hash, source transitions, business before-images, current owner state, producer lineage, and parent presence. [`V2BusinessRows.java`](../transaction-apply/src/main/java/com/example/cdc/apply/V2BusinessRows.java) contains the four whitelisted mappings. Business mutations, owner/producers, and `cdc.v2_receipt` commit together.

The local lab uses one apply worker, so global advisory locks, per-owner logical-lock rows, and `SKIP LOCKED` multi-worker coordination are intentionally removed.

## State retained

Projector checkpoints retain immutable observations, producer claims, pending/completed transactions, graph edges, wait reasons, and DLQ decisions. PostgreSQL retains staged transaction/mutation/dependency rows plus owner, producer, and receipt evidence.

See [transaction-working-example.md](transaction-working-example.md) for a complete example and [transaction-protocol-operations.md](transaction-protocol-operations.md) for commands.
