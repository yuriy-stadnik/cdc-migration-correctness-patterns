# Transaction-Preserving CDC — Request 2 Implementation Record

Status: **Historical Request 2 implementation record; Request 3 release validation is blocked**

Implementation date: **2026-10-06**

Approved design: [`design/transaction-protocol-design.md`](design/transaction-protocol-design.md)

Operations guide: [`transaction-protocol-operations.md`](transaction-protocol-operations.md)

Current validation results, fixes, remaining failures, and manual test instructions are recorded in [Request 3 validation](transaction-protocol-request3-validation.md). Its findings supersede the earlier verification claims below: 39 of 41 saved tests pass, with source replay and manifest descriptor integrity still failing.

## 1. Implemented guarantee

The implementation provides:

> at-least-once transport + replay-safe idempotent effects + atomic destination PostgreSQL transaction

It does not claim end-to-end exactly-once delivery. Kafka and MirrorMaker2 may redeliver records. The destination handles replay through immutable message identities, mutation identities, idempotency constraints, exact version checks, and an applied-transaction receipt committed with the business changes.

The strict atomicity boundary is one logical PostgreSQL/Aurora database containing the `client`, `operational`, and `cdc` schemas.

## 2. Implemented architecture

```text
PostgreSQL
  → Debezium transaction metadata and row changes
  → cdc.source.capture.v1 (one ordered partition)
  → Flink transaction assembly and deterministic projection
  → cdc.tx.control.v1 + cdc.tx.mutations.v1 + cdc.tx.dlq.v1
  → MirrorMaker2 / MSK
  → durable PostgreSQL inbox
  → completeness and dependency validation
  → PostgreSQL-backed READY scheduler
  → atomic destination transaction apply
  → applied receipt + version history + outbox
```

### Source capture

Debezium is configured with transaction metadata and a `RegexRouter` SMT that routes captured rows and transaction boundary events into `cdc.source.capture.v1`. The topic has one partition so the PostgreSQL adapter has one ordered planning lane.

Producer idempotence, `acks=all`, and one in-flight request are configured. These settings strengthen transport behavior but are not presented as an end-to-end exactly-once guarantee.

### Flink projector

The Java DataStream job replaces the old Flink SQL fan-out as the authoritative path. It:

- normalizes Debezium PostgreSQL transaction IDs whose LSN suffix can vary;
- assembles BEGIN, data, and END fragments;
- validates explicit event counts and contiguous transaction record sequences;
- ignores identical duplicate fragments without increasing the count;
- rejects conflicting duplicate fragments;
- blocks the planning frontier behind an incomplete earlier transaction;
- publishes INCOMPLETE after ten minutes and DLQ after 24 hours;
- checkpoints assembly, projected object versions, payload state, and source progress;
- projects one net destination mutation per object;
- handles null contribution removal and destination-key changes;
- quarantines conflicting projected contributions;
- emits immutable manifests and numbered mutation records through an exactly-once Flink Kafka sink.

### Destination engine

Kafka ingestion and business application are separate durable steps.

Each protocol message is first inserted into `cdc.inbox_message`. The local consumer commits only the exact Kafka partition offset that was durably staged. Lambda throws on staging or apply failure so the MSK event source does not treat an error-shaped response as successful processing.

After staging, PostgreSQL determines whether the transaction is incomplete, waiting for dependencies, or READY. Apply workers use the same shared Java core in local and AWS modes.

## 3. Wire protocol

The shared protocol module defines:

- canonical transaction identity;
- source database, incarnation, and capture scope;
- native transaction ID;
- transaction record sequence and count;
- source commit position and ordered capture sequence;
- entity and typed primary key;
- INSERT, UPDATE, and DELETE operation;
- stable mutation and idempotency identities;
- optional correlation ID on the manifest;
- structured source event positions;
- before/current, expected, and resulting object versions;
- dependencies and transaction lifecycle messages;
- deterministic canonical JSON and SHA-256 hashes.

Object versions have this semantic form:

```text
{ generation, producerTransactionId, mutationSequence, presence }
```

They identify the transaction mutation that produced a projected object state. Kafka offsets, timestamps, and Debezium per-transaction ordering fields are not object versions.

## 4. Kafka topics

| Topic | Purpose | Local partitions |
|---|---|---:|
| `cdc.source.capture.v1` | Ordered source planning lane; not mirrored | 1 |
| `cdc.tx.control.v1` | Immutable manifests and lifecycle control | 12 |
| `cdc.tx.mutations.v1` | Immutable numbered mutation payloads | 12 |
| `cdc.tx.dlq.v1` | Quarantined source protocol failures | 3 |

Control and mutation records are keyed by canonical transaction ID. Destination correctness does not depend on independent topic arrival order.

## 5. Database migration

Migration: [`../db/migration/V001__transaction_protocol.sql`](../db/migration/V001__transaction_protocol.sql)

Business tables receive `cdc_version`, `source_tx_id`, and unique idempotency fields.

The migration creates:

- `cdc.schema_version`
- `cdc.inbox_message`
- `cdc.tx_transaction`
- `cdc.mutation`
- `cdc.dependency`
- `cdc.object_state`
- `cdc.version_history`
- `cdc.applied_transaction`
- `cdc.apply_attempt`
- `cdc.outbox`
- `cdc.quarantine`
- `cdc.transaction_dependency_graph`

Runtime schema migration is protected by a PostgreSQL advisory lock so concurrent Lambda cold starts cannot run the migration concurrently.

## 6. Transaction state machine

Implemented destination states:

```text
COLLECTING → COMPLETE → WAITING_DEPENDENCIES → READY → APPLYING → APPLIED

COLLECTING → INCOMPLETE
INCOMPLETE → COLLECTING → COMPLETE
APPLYING → RETRYABLE → READY
eligible nonterminal state → FAILED → DLQ
```

COMPLETE can be an internal transition within the same staging database transaction before the externally visible READY or WAITING state.

## 7. Dependency and DAG behavior

The implementation persists:

- write-predecessor dependencies derived from expected object versions;
- explicit predecessor transaction references represented by the protocol;
- FK existence dependencies;
- unresolved producer references whose transaction manifest has not arrived;
- current object presence and version evidence.

An ordinary write edge is:

```text
TX2 expects object X at version V
TX1 produced X@V
⇒ TX1 → TX2
```

Recursive SQL detects cycles. Transactions participating in a discovered cycle are marked FAILED and quarantined; an edge is never removed to make the graph executable.

The `cdc.transaction_dependency_graph` view exposes predecessor, dependent, dependency type, required version, current version, presence, states, and an `as_of` timestamp.

## 8. Destination transaction algorithm

The apply worker performs:

```text
BEGIN
  claim the oldest eligible transaction FOR UPDATE SKIP LOCKED
  check the immutable applied receipt
  validate manifest count, sequence coverage, and mutation-set hash
  create proven baseline lock rows when permitted
  lock affected object-state rows in canonical order
  revalidate dependencies and exact expected versions
  apply every INSERT / UPDATE / DELETE
  conditionally advance every object version
  write durable deletion tombstones
  insert version history
  insert the applied-transaction receipt
  mark the transaction APPLIED
  insert the outbox event
COMMIT
```

UPDATE and DELETE statements include a `cdc_version = expectedVersion` predicate and require exactly one affected row. Object-state advancement also uses the expected version as a conditional predicate.

Any SQL or validation failure rolls back the entire database transaction. A failed transaction cannot leave partial business rows, versions, history, receipt, or outbox state.

## 9. Concurrency protection

- `FOR UPDATE SKIP LOCKED` claims independent READY transactions.
- Transaction-row locks prevent simultaneous ownership of the same transaction.
- Object-state rows are locked in canonical order.
- A unique applied receipt prevents two workers from successfully committing the same source transaction.
- Unique mutation, message, and idempotency identities protect against duplicate effects.
- No in-memory lock is used as the cross-instance correctness mechanism.
- Independent transactions can be applied by different workers concurrently.

## 10. Recovery behavior

- Flink restores source offsets and assembly/version state from checkpoints.
- Replayed source transactions reproduce the same identities and hashes.
- Identical inbox, manifest, and mutation records are no-ops.
- Same identity with different content is quarantined.
- Missing fragments never become successfully applied transactions.
- Valid late fragments can recover an INCOMPLETE transaction before DLQ.
- DLQ work requires an audited operator decision before requeue.
- A worker crash before COMMIT rolls back changes and releases database locks.
- A retry after an uncertain COMMIT checks the applied receipt first.
- Transient SQL failures enter RETRYABLE with an initial five-second backoff.
- Local workers sweep continuously; EventBridge invokes Lambda every minute so retry progression does not require a new Kafka record.
- Correctness metadata has no automatic TTL until a replay floor is established.

## 11. Production files changed

### Protocol

- `transaction-protocol/build.gradle`
- `transaction-protocol/settings.gradle`
- `transaction-protocol/src/main/java/com/example/cdc/protocol/Protocol.java`
- `transaction-protocol/src/main/java/com/example/cdc/protocol/ProtocolCodec.java`

### Flink transaction projector

- `transaction-projector/Dockerfile`
- `transaction-projector/build.gradle`
- `transaction-projector/settings.gradle`
- `transaction-projector/src/main/java/com/example/cdc/projector/AssemblyRules.java`
- `transaction-projector/src/main/java/com/example/cdc/projector/CaptureDeserializer.java`
- `transaction-projector/src/main/java/com/example/cdc/projector/CaptureRecord.java`
- `transaction-projector/src/main/java/com/example/cdc/projector/InventoryProjectionPlanner.java`
- `transaction-projector/src/main/java/com/example/cdc/projector/OutputRecord.java`
- `transaction-projector/src/main/java/com/example/cdc/projector/ProjectorState.java`
- `transaction-projector/src/main/java/com/example/cdc/projector/TransactionProjectorFunction.java`
- `transaction-projector/src/main/java/com/example/cdc/projector/TransactionProjectorJob.java`

### Destination apply engine

- `transaction-apply/build.gradle`
- `transaction-apply/settings.gradle`
- `transaction-apply/src/main/java/com/example/cdc/apply/ConnectionProvider.java`
- `transaction-apply/src/main/java/com/example/cdc/apply/SchemaMigrator.java`
- `transaction-apply/src/main/java/com/example/cdc/apply/TransactionApplyService.java`
- `transaction-apply/src/main/java/com/example/cdc/apply/VersionConflictException.java`

### Runtime and deployment

- `lambda_java/src/main/java/com/example/Handler.java`
- `local/cloud/lambda-consumer/src/main/java/com/example/local/LocalLambdaConsumer.java`
- `local/source/connectors/legacy-postgres-source-config.json`
- `local/source/docker-compose.yml`
- `local/source/mm2.properties`
- `local/cloud/docker-compose.yml`
- `local/cloud/lambda-consumer/Dockerfile`
- `aurora.tf`
- `lambda.tf`
- `ec2.tf`
- `artifacts.tf`
- `variables.tf`
- `outputs.tf`
- `user_data.sh.tpl`
- `local_deployment.tf`

### Tests and documentation

- protocol, projector, apply-engine, Lambda, and local-worker JUnit tests;
- local component test scripts;
- `local/scripts/run-local-e2e.sh`;
- `README.md`;
- approved design and operations documentation.

## 12. Verification evidence

Passed locally:

| Area | Tests | Result |
|---|---:|---|
| Protocol | 4 | Passed |
| Projector and assembly | 5 | Passed |
| Real embedded PostgreSQL apply tests | 3 | Passed |
| Lambda | 2 | Passed |
| Local worker | 1 | Passed |
| **Total** | **15** | **0 failures, 0 skipped** |

The real PostgreSQL component tests covered:

- atomic multi-entity application;
- replay idempotency;
- stale expected-version rejection;
- full rollback after a deferred-FK failure at commit;
- concurrent application by two independent workers.

Additional successful checks:

- Flink projector fat-JAR build;
- Lambda fat-JAR build;
- local worker fat-JAR build;
- `terraform validate`;
- Terraform formatting;
- local source and destination Compose configuration validation;
- shell syntax validation;
- `git diff --check`.

Generated artifacts were approximately:

- `transaction-projector-all.jar`: 19 MB;
- `msk-to-aurora-lambda-1.0.0.jar`: 14 MB;
- `local-lambda-consumer-all.jar`: 21 MB.

The Docker end-to-end stack was not executed because the Docker daemon was unavailable. AWS deployment validation was intentionally not performed.

## 13. Known limitations and differences from the approved design

1. Manifest paging is not implemented. V1 uses one manifest header with a mutation-set hash. Transaction and message size limits must be established before production rollout.
2. Snapshot records currently become deterministic single-record bootstrap transactions. A controlled, atomically activated baseline remains necessary for production cutover with existing destination state.
3. Shared-product conflicts in one transaction are quarantined, but complete contribution withdrawal and product deletion are not inferred from deletion of one denormalized `orders_flat` row.
4. Explicit application read-set dependencies are represented by protocol types but are not available from ordinary Debezium row events.
5. The outbox is written atomically, but a status/DLQ outbox publisher and audited requeue command are not included.
6. PostgreSQL is implemented. Oracle SCN and transaction-identity behavior remains unimplemented and uncertified.
7. The lab projector checkpoints one serialized keyed planner value. Production scale requires checkpoint-state, transaction-size, and Kafka-message-size validation.
8. Existing older destination clusters are not migrated in place. The Terraform change creates the approved consolidated destination topology; an existing-data migration requires an explicit cutover plan.

These limitations prevent claiming that the implementation is ready for an uncontrolled production cutover. They do not change the tested atomic apply, idempotency, exact-version, and cross-worker concurrency behavior for complete protocol transactions.
