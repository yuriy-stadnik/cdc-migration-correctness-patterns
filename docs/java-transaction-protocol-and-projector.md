# Java transaction protocol and projector

This guide explains the Java code in `transaction-protocol` and `transaction-projector`. The protocol module defines and validates the records exchanged between components. The projector module consumes raw Debezium capture records and produces complete transaction manifests and mutations.

## 1. End-to-end position

```text
Source PostgreSQL
  -> Debezium
  -> cdc.source.capture.v2
  -> transaction-projector
       -> cdc.tx.mutations.v2
       -> cdc.tx.control.v2
       -> cdc.tx.dlq.v2
  -> MirrorMaker 2
  -> destination apply worker
  -> destination PostgreSQL
```

The projector does not write destination business tables. Its responsibility ends after it publishes a verified transaction description. The destination apply service later stages and commits that description atomically.

## 2. `transaction-protocol` module

The protocol module has no Flink or Kafka client logic. It is a shared Java library used by the projector and destination apply code.

### 2.1 `ProtocolV2`

File: [`ProtocolV2.java`](../transaction-protocol/src/main/java/com/example/cdc/protocol/ProtocolV2.java)

This class defines the only supported wire contract.

Topic constants:

| Constant | Topic | Purpose |
|---|---|---|
| `CAPTURE_TOPIC` | `cdc.source.capture.v2` | Raw ordered Debezium input |
| `CONTROL_TOPIC` | `cdc.tx.control.v2` | Transaction manifests |
| `MUTATION_TOPIC` | `cdc.tx.mutations.v2` | Destination row mutations |
| `BASELINE_TOPIC` | `cdc.tx.baseline.v2` | Baseline owner records |
| `DLQ_TOPIC` | `cdc.tx.dlq.v2` | Invalid capture evidence |

Important record types:

| Record | Meaning |
|---|---|
| `TypedValue` | Canonical typed primary-key value, such as `int64` or `text` |
| `OwnerIdentity` | Source database, incarnation, table, and primary key |
| `ObjectIdentity` | Destination table and primary key |
| `OwnerState` | Row presence and numeric version |
| `ProducerReference` | Baseline or transaction that produced a version |
| `Predecessor` | Exact source state required before a mutation |
| `Dependency` | Required predecessor or foreign-key state |
| `JournalRevision` | One numeric source-version transition |
| `OwnerTransition` | Complete before-to-after change for one source row |
| `Mutation` | One destination row operation |
| `Manifest` | Complete description of one source transaction |
| `BaselineOwner` | Initial known state of one source owner |
| `Baseline` | Baseline identity, boundary, and owner count |

The record constructors enforce protocol invariants immediately. Invalid objects cannot be created normally. Examples include:

- protocol and schema versions must match;
- only the six normalized source and destination tables are allowed;
- source and destination primary keys must have the expected fields and types;
- `ABSENT` has no numeric version, while `PRESENT` and `DELETED` require a positive version;
- source versions must advance without gaps;
- a source owner must map to the destination row with the same primary key;
- inserts, updates, and deletes must have consistent before/after presence and payloads.

### 2.2 `ProtocolV2Codec`

File: [`ProtocolV2Codec.java`](../transaction-protocol/src/main/java/com/example/cdc/protocol/ProtocolV2Codec.java)

The codec provides deterministic JSON encoding and full record verification.

Canonical JSON rules include:

- object properties are sorted;
- map entries are sorted;
- decimals use a normalized plain representation;
- scalar coercion, unknown fields, trailing tokens, and floating-point-to-integer conversion are rejected.

These rules make the same logical record produce the same serialized representation during replay.

Transaction identity is generated from the durable source UUID:

```text
source-v2:[sourceDatabaseId, sourceIncarnation, sourceTransactionUuid]
```

It does not use Kafka offsets or a reusable PostgreSQL native XID as the permanent transaction identity.

For every mutation, the codec derives:

```text
mutationId     = transactionId + ":" + mutationSequence
idempotencyKey = projectionGeneration + ":" + mutationId
```

`mutationSetHash` is SHA-256 over the complete canonical mutation descriptors, ordered by mutation sequence. Changing a version, dependency, operation, key, or payload changes the hash.

The methods named `signMutation` and `signManifest` do not create a cryptographic signature. They fill deterministic identities and the integrity digest, then validate the completed records.

### 2.3 Simplified output structure

A mutation contains:

```text
Mutation
  transactionId
  mutationSequence
  owner                 source table and PK
  object                destination table and same PK
  operation             INSERT, UPDATE, or DELETE
  expected              exact predecessor state and producer
  result                new presence and numeric version
  dependencies[]        predecessor and FK requirements
  before / after        row payloads
  mutationId
  idempotencyKey
```

A manifest contains:

```text
Manifest
  transactionId
  source identity and capture scope
  active baselineId
  source BEGIN/COMMIT boundary
  journal and captured-event counts
  sourceTransitions[]
  destinationDescriptorCount
  mutationSetHash
```

The manifest allows the destination to confirm that it has the entire mutation set before applying anything.

## 3. `transaction-projector` module

The projector is a Flink DataStream application. It is Java code, despite the historical Compose service name `source-flink-sql-init`.

### 3.1 Production classes

| Class | Responsibility |
|---|---|
| [`TransactionProjectorJob`](../transaction-projector/src/main/java/com/example/cdc/projector/TransactionProjectorJob.java) | Main class; creates Kafka source, keyed processing function, and Kafka sink |
| [`CaptureRecord`](../transaction-projector/src/main/java/com/example/cdc/projector/CaptureRecord.java) | Internal Kafka envelope containing topic, partition, offset, timestamp, and JSON value |
| [`CaptureDeserializer`](../transaction-projector/src/main/java/com/example/cdc/projector/CaptureDeserializer.java) | Converts Kafka consumer records into `CaptureRecord` objects |
| [`V2TransactionProjectorFunction`](../transaction-projector/src/main/java/com/example/cdc/projector/V2TransactionProjectorFunction.java) | Flink state wrapper around the deterministic planner |
| [`V2ProjectorState`](../transaction-projector/src/main/java/com/example/cdc/projector/V2ProjectorState.java) | Checkpointed baseline, observations, producer index, pending transactions, graph, completed output, and quarantine state |
| [`V2Capture`](../transaction-projector/src/main/java/com/example/cdc/projector/V2Capture.java) | Parses and validates PostgreSQL/Debezium transaction evidence |
| [`V2Projector`](../transaction-projector/src/main/java/com/example/cdc/projector/V2Projector.java) | Builds dependencies, mutations, manifests, and DLQ records |
| [`OutputRecord`](../transaction-projector/src/main/java/com/example/cdc/projector/OutputRecord.java) | Output topic, Kafka key, and serialized value |

### 3.2 Application startup

`TransactionProjectorJob.main()` performs these steps:

1. Reads Kafka and source-scope configuration from environment variables.
2. Forces parallelism to one verified planning lane.
3. Enables exactly-once Flink checkpoints every ten seconds.
4. Reads the capture topic and both baseline input topics from the earliest available offsets.
5. Uses Kafka `read_committed` isolation.
6. Keys all records for one database/incarnation/scope into the same stateful function.
7. Sends produced `OutputRecord` values through an exactly-once Kafka sink.

The Docker image packages the application as `transaction-projector-all.jar`. The Compose initialization service submits `TransactionProjectorJob` to the Flink JobManager. The TaskManager executes the processing function.

### 3.3 Capture validation and transaction assembly

`V2Capture` treats PostgreSQL log positions as source ordering evidence. Kafka offsets are transport information, not source versions or transaction identity.

For each committed transaction it verifies:

- matching BEGIN and END records;
- a single transaction occurrence interval;
- exact event count and per-table counts;
- continuous `total_order` values;
- consistent PostgreSQL XID and LSN evidence;
- one captured `source_cdc.transaction_context` row containing the stable UUID;
- append-only `source_cdc.revision_journal` entries;
- exact journal count and continuous ordinals;
- agreement between business-row before/after images and journal revisions.

Multiple changes to the same row inside one transaction are reduced to one net owner transition while retaining the complete ordered revision chain.

### 3.4 Baseline activation

The projector does not assume that a missing row means `ABSENT`. It waits for an activated, reconciled baseline.

Baseline processing:

1. Baseline-owner records populate the initial owner/version index.
2. The baseline control record declares the expected owner count and PostgreSQL boundary.
3. The projector validates scope, counts, payload versions, and uniqueness.
4. When all owners are present, the baseline becomes immutable and active.
5. Live transactions at or before the baseline boundary are rejected as unresolved overlap.

### 3.5 Version producers and transaction ordering

Every known owner revision is indexed by source owner and numeric version. Its producer is either:

- `BASELINE` with the baseline ID; or
- `TRANSACTION` with the canonical transaction ID.

Before planning a row change, the projector finds the producer of the exact previous version. If it is missing, the transaction remains pending. If another transaction produced it, the projector creates a graph edge and waits until that predecessor transaction is completed.

The graph is checked for cycles. A cycle or conflicting producer claim causes quarantine instead of arbitrary ordering.

### 3.6 Dependencies

The current projector emits two dependency types:

- `WRITE_PREDECESSOR`: required for every changed owner; proves the exact previous row version and presence.
- `EXISTS`: required when a destination foreign key refers to a parent outside the same source transaction.

Foreign-key relationships are:

```text
address         -> customer
contact number  -> customer
order           -> customer
order item      -> order
order item      -> product
```

If parent and child are both created in the same transaction, no external `EXISTS` dependency is needed because the destination applies the complete mutation set atomically with deferred foreign keys.

Before allowing a parent deletion, the projector checks known child state at the source commit boundary. It rejects a deletion that would leave a surviving child.

### 3.7 Direct row mapping

There are no source projection tables or multi-row aggregation rules. Mapping is one-to-one:

| Source owner | Destination object |
|---|---|
| `inventory.customers` | `client.customers` |
| `inventory.addresses` | `client.addresses` |
| `inventory.contact_numbers` | `operational.contact_numbers` |
| `inventory.products` | `operational.products` |
| `inventory.orders` | `operational.orders` |
| `inventory.order_items` | `operational.order_items` |

The source and destination use the same typed primary key.

### 3.8 Publication

For a valid transaction, `V2Projector.plan()`:

1. Creates all owner transitions.
2. Builds predecessor and foreign-key dependencies.
3. Creates one signed/verified mutation for every changed destination row.
4. Creates one manifest covering the complete mutation set.
5. Publishes mutations to `cdc.tx.mutations.v2`.
6. Publishes the manifest to `cdc.tx.control.v2`.

All records use the canonical transaction ID as their Kafka key.

For invalid evidence, the projector records the failure in checkpoint state and publishes one diagnostic record to `cdc.tx.dlq.v2`. A conflict affecting an already completed transaction can halt further planning for that lane.

## 4. Checkpoint and replay behavior

`V2TransactionProjectorFunction` stores the complete `V2ProjectorState` as canonical JSON in Flink `ValueState` after every input record.

The state includes:

- active and candidate baselines;
- immutable capture observations;
- version-producer claims;
- pending and completed transactions;
- dependency graph edges;
- previously produced output;
- waiting and quarantine reasons.

On replay, semantically identical evidence is accepted. Conflicting reuse of the same observation, owner revision, source UUID, or completed transaction identity is quarantined. The Kafka sink participates in Flink exactly-once checkpoints, while the complete system guarantee remains at-least-once transport with replay-safe destination effects.

## 5. Example

A source transaction creates a customer, address, product, order, and order item.

```text
Source transaction UUID
  ├─ customer version 1
  ├─ address version 1
  ├─ product version 1
  ├─ order version 1
  └─ order-item version 1
```

The projector emits five mutations and one manifest. Because all parents are created in the same transaction, they are validated from the transaction's final states and applied together. If the order instead referenced an existing customer, the order mutation would include an `EXISTS` dependency pointing to the baseline or transaction that produced the customer's required version.

## 6. Tests and build commands

Protocol tests cover canonical round trips, complete-descriptor hashing, one-to-one key enforcement, and baseline verification:

```bash
gradle -p transaction-protocol test
```

Projector tests cover multi-update reduction, direct address ownership, predecessor waiting, foreign-key dependencies, duplicate evidence, and replay of captured Debezium envelopes:

```bash
gradle -p transaction-projector test
gradle -p transaction-projector fatJar
```

The full local path is exercised with:

```bash
RESET=1 ./local/scripts/run-local-e2e.sh
```

## 7. Recommended reading order

1. `ProtocolV2.java` — understand the wire records and invariants.
2. `ProtocolV2Codec.java` — understand canonical identity and integrity validation.
3. `TransactionProjectorJob.java` — understand Flink and Kafka wiring.
4. `V2TransactionProjectorFunction.java` and `V2ProjectorState.java` — understand checkpoint ownership.
5. `V2Capture.java` — understand Debezium transaction reconstruction.
6. `V2Projector.java` — understand dependency planning and output publication.
