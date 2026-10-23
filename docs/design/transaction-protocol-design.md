# Transaction Protocol, Semantics, and Invariants

Status: **Approved defaults implemented by Request 2 on 2026-10-06**

Scope: Approved protocol baseline. Implementation status and operations are documented in `docs/transaction-protocol-operations.md`.

Repository baseline reviewed: `f4c07c1` and the working tree at the time of assessment.

The recommended design is a transaction-aware source projector, immutable transaction manifests, and a Java apply engine backed by PostgreSQL queue and version tables.

The most consequential decision is the destination atomicity boundary: **to make all changes from one source transaction atomically visible, the affected `client` and `operational` tables must be in one logical PostgreSQL/Aurora database.** Keeping the current two databases supports atomicity within each database and recoverable partial delivery across them, but it cannot satisfy the stronger visibility guarantee.

This is a read-only design assessment of the current working tree at `f4c07c1`. No implementation files were changed, and no builds, tests, deployments, or live database operations were run. The code-verification workflow informed the evidence review and the acceptance criteria below.

## 1. Current architecture assessment

The repository implements PostgreSQL CDC; Oracle support is not currently present.

| Layer | Current implementation | Consequence for the proposed protocol |
|---|---|---|
| Source | PostgreSQL 16; `inventory.customers`, `accounts`, `orders_flat`; `REPLICA IDENTITY FULL` | Before/after row data is available, but no application row-version column exists. |
| Debezium | `2.7.3.Final`, `pgoutput`, one connector task, initial snapshot, transaction metadata enabled | Useful raw transaction information exists. Snapshot records need separate treatment. |
| Source topics | Three `pg1.inventory.*` topics and `pg1.transaction` | Source transactions span topics; downstream arrival order is insufficient for reconstruction. |
| Flink | `1.19.1`; JSON sources; separate SQL inserts into six business topics | Transformation fans out source records and discards important source-position information. |
| Replication | MM2 with identity topic naming and exactly-once-related settings | Replication configuration does not establish one destination database transaction. |
| Destination | Java Lambda and a separate local Java consumer | Both apply individual records immediately. |
| Storage | Separate client and operational databases | No shared atomic commit boundary. |
| Existing control state | Transaction metadata, processed events, postponed FK events | Audit and record retry facilities exist; transaction assembly, object versions, and DAG scheduling do not. |

Configuration evidence: [Debezium connector](../../local/source/connectors/legacy-postgres-source-config.json), [source deployment](../../local/source/docker-compose.yml), [Flink transformations](../../local/source/flink/sql/init.sql), and [MM2 configuration](../../local/source/mm2.properties).

Several existing behaviors materially affect correctness:

- **The writers commit per business record.** `processBusinessEvent()` wraps one upsert and associated retry work in a database transaction. It does not collect the source transaction. [Handler.java](../../lambda_java/src/main/java/com/example/Handler.java), reviewed line 404.
- **Transaction metadata is audit-only.** Receiving END does not validate completeness or control application. [Handler.java](../../lambda_java/src/main/java/com/example/Handler.java), reviewed line 369.
- **The ordering guard is invalid across transactions.** Upserts compare `source_tx_total_order`, which is an intra-transaction sequence. A later transaction’s record 1 can be suppressed after an earlier transaction’s record 10, while the processed-event marker is still committed. [Handler.java](../../lambda_java/src/main/java/com/example/Handler.java), reviewed line 184.
- **Deletes are not implemented as business deletes.** The operation is stored as metadata, but routing invokes upserts.
- **FK postponement breaks whole-transaction atomicity.** A failed child write can be marked processed and applied later, potentially inside a different transaction.
- **Flink’s `COALESCE(after.field, before.field)` loses intentional null updates.** Its fan-out also needs explicit rules for disappearing phone/address values and changed target keys.
- **Acknowledgment needs correction.** The local consumer calls parameterless `commitSync()` inside its record loop, although Kafka’s consumer position advances for the returned poll batch. AWS catches exceptions and returns an ordinary error map, which does not provide the invocation failure signaling required for retries. [LocalLambdaConsumer.java](../../local/cloud/lambda-consumer/src/main/java/com/example/local/LocalLambdaConsumer.java), reviewed line 76; [Handler.java](../../lambda_java/src/main/java/com/example/Handler.java), reviewed line 108; [Kafka consumer contract](https://kafka.apache.org/37/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html); [Lambda MSK processing](https://docs.aws.amazon.com/lambda/latest/dg/with-msk-configure.html).

There is also a significant connector-specific identity issue. The pinned PostgreSQL transaction monitor constructs the exposed transaction ID from the native transaction ID **and the currently observed LSN**, including for BEGIN, data, and END. Consequently, the complete raw `transaction.id` string must not be assumed identical throughout a transaction. [Debezium 2.7.3 implementation](https://github.com/debezium/debezium/blob/v2.7.3.Final/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/pipeline/txmetadata/PostgresTransactionMonitor.java)

Existing tests cover helpers, mocked JDBC interactions, eventual row delivery, and metadata presence. They do not establish transaction atomicity, version correctness, or crash recovery. The AWS E2E script creates a multi-table source transaction but checks destination rows independently. [AWS E2E assertions](../../scripts/run-aws-e2e-test.sh), reviewed line 146.

## 2. Guarantee and scope

The proposed strict mode guarantees:

1. At-least-once transport, assuming retained source and Kafka history remains available.
2. Complete reconstruction of the **captured portion** of a committed source transaction.
3. Deterministic projection into destination mutations.
4. Atomic application of those mutations within one destination database.
5. Idempotent transaction effects under replay.
6. Exact expected-version validation.
7. Dependency-respecting execution, with concurrency for independent transactions.

It does not claim:

- Atomic commit between Kafka and PostgreSQL.
- Atomic visibility across separate databases.
- Reconstruction of writes excluded by the connector’s capture filters.
- Recovery of source reads that CDC never recorded.
- Reproduction of the source’s complete serialization history.
- Unlimited historical replay after its required evidence has been deleted.

Independent transactions can commit in a different order at the destination. This preserves the declared dependency model, but does not promise that every destination snapshot equals a prefix of source commit order.

Readers performing several related queries need their own consistent database snapshot if they require a coherent multi-query view.

## 3. Recommended architecture

```text
Debezium
  → ordered source capture log
  → Flink transaction assembly
  → deterministic projection + predecessor-version assignment
  → immutable control manifests + mutation records
  → MM2/MSK
  → durable destination inbox
  → PostgreSQL dependency/READY queue
  → Java apply workers
  → business rows + versions + applied receipt in one DB commit
```

The source stage and destination stage have different responsibilities:

- The source stage determines **what the committed transaction means**, including its mutations and predecessor versions.
- The destination stage verifies **what has actually arrived and been applied**.

A destination worker must never manufacture an expected version from whatever happens to be present when a record arrives.

For the simplest reliable first version, I recommend **one ordered capture lane per source database/capture scope**:

- Route the connector’s captured data and transaction-boundary events into one append-only, single-partition capture topic.
- Preserve the original source table, key, envelope, and boundary type.
- Use one active connector task and an ordering-preserving producer configuration.
- Keep the partition count fixed for that capture generation.
- Run one logical source planner per lane.
- Permit parallel destination application after dependencies have been established.

Kafka Connect topic routing can support the topic consolidation; producer idempotence and compatible settings preserve producer ordering through retries. Their applicability to all data and boundary records must be verified with the pinned connector. [Topic routing](https://docs.confluent.io/kafka-connectors/transforms/current/regexrouter.html), [Kafka producer ordering](https://kafka.apache.org/38/configuration/producer-configs/)

This lane supplies ordering evidence. Transaction completeness checks and database atomic commits are still required.

The cost is a source-side throughput bottleneck and possible blocking behind an incomplete transaction. Given this prototype already uses parallelism 1, it is a reasonable starting tradeoff. Scaling assembly across the existing raw topics is possible later, but requires certified transaction identity and a completeness frontier; simply sorting the transactions currently received is unsafe.

## 4. Identity, positions, and ordering

Use separate identifiers for separate purposes:

| Field | Meaning |
|---|---|
| `source_database_id` | Stable registered identity of the logical source, independent of hostname or connector deployment name. |
| `source_incarnation` | Identifies a continuous source history. Changes after an incompatible restore, fork, or reinitialization; ordinary restarts do not change it. |
| `capture_scope_id` | Versioned definition of included tables/events and capture rules. |
| `native_transaction_id` | PostgreSQL XID or Oracle transaction identifier; retained as source evidence, not globally unique alone. |
| `transaction_id` | Canonical identity derived from source identity, incarnation, native transaction, and a verified commit-boundary discriminator. |
| `source_event_position` | Native location of a particular source change, including any required tie-breaker. |
| `source_record_sequence` | Original order within the captured transaction. |
| `source_commit_position` | Adapter-verified commit/boundary position, with an explicit position type. |
| `source_commit_sequence` | Monotonic sequence assigned once to committed transactions in this capture lane. |
| `projection_generation` | Identifies a compatible projection/version baseline. |
| `mutation_sequence` | Deterministic order of derived destination mutations within the transaction. |

Rules:

- Retain raw Debezium IDs for diagnosis, but normalize them through a source-specific adapter.
- Provisional grouping by native XID is allowed only within a verified transaction occurrence. XID reuse must not merge historical transactions.
- The PostgreSQL adapter must distinguish event LSN from the verified transaction boundary position. Do not label arbitrary `source.lsn` values “commit LSN.”
- Oracle needs a separate adapter for native transaction identity, SCN/commit-SCN, and event discriminators. Those fields exist in its connector contract, but this repository does not yet exercise them. [Oracle connector metadata](https://debezium.io/documentation/reference/2.7/connectors/oracle.html)
- LSN/SCN values are serialized losslessly, preferably as strings with a typed numeric comparator. Do not compare arbitrary strings lexicographically.
- `source_commit_sequence` orders one source incarnation/capture lane. It is not a sequence shared across PostgreSQL and Oracle.
- Kafka offsets identify transport progress. They are not object versions.

Assign the commit sequence when an authenticated END occurrence is recognized in the ordered source lane. Persist the transaction-to-sequence assignment. Replays reuse the assignment.

The planner may advance only through a contiguous, complete sequence. A later transaction cannot overtake an earlier incomplete transaction while predecessor versions are being calculated.

## 5. Transaction assembly and projection

Assembly has two independent completeness checks.

**Source completeness**

A source transaction is complete only when:

- Its canonical occurrence is unambiguous.
- Required BEGIN and END metadata are linked.
- END identifies a committed transaction.
- All distinct captured records are present.
- Record sequences and per-collection counts agree with the connector contract.
- Duplicate fragments agree in canonical content.
- The capture scope and schema versions are consistent.

Debezium supplies END counts and intra-transaction ordering metadata; those counts describe emitted source events, not arbitrary downstream projections. Initial snapshots require separate handling. [PostgreSQL transaction metadata](https://debezium.io/documentation/reference/2.7/connectors/postgresql.html#postgresql-transaction-metadata)

For this repository, one account record can produce an address and several phone records. One flat-order record can affect a product, order, and order item. Therefore:

```text
captured_source_event_count ≠ expected_destination_mutation_count
```

Never reuse one count for the other.

**Projection completeness**

After source assembly:

1. Process source records in their original sequence.
2. Update a deterministic source-row/contribution model.
3. Compute the resulting destination projection.
4. Compare the transaction’s initial and final projected states.
5. Emit a deterministic mutation set.
6. Assign expected/resulting versions.
7. Seal the manifest with counts and hashes.

I recommend **one net mutation per destination object per source transaction**, while preserving the original source records and sequence as lineage. An object repeatedly updated inside one transaction produces one final destination transition.

This preserves committed projected state. It does not reproduce every intermediate statement or destination trigger invocation. If intermediate operations have required side effects, they need a different approved contract.

The contribution model is necessary because the current transformation is many-to-one:

- Several `orders_flat` rows can contribute to the same product name.
- Several accounts can map to the same customer/address-type or customer/phone-type key.

Deleting one order must not automatically delete a product still contributed by another order. Similarly, a changed customer key or a phone becoming null can require removing an old contribution.

Recommended default: preserve contribution ownership, reject conflicting contributions until an entity-specific resolution rule is approved, and remove a projected object only according to that rule. Arbitrary “last message wins” would conceal a business decision.

Flink state must include transaction fragments, source progress, deduplication evidence, contributions, planned object versions, and sequence assignments. Configure durable checkpoints and restore procedures. Timers detect delay; they never declare missing data complete.

## 6. Object-version semantics

The choices are not interchangeable:

| Choice | Meaning and origin | Replay/comparison properties | Suitability |
|---|---|---|---|
| Application row version | Source application increments a row revision | Stable if every writer follows the rule; compare exact expected revision | Best when available, but absent here and insufficient alone for many-to-one projections |
| Source transaction/global sequence | Position of a transaction in an ordered source history | Stable only if source-assigned or durably assigned; does not identify an object’s predecessor by itself | Useful transaction ordering metadata |
| Entity-wide monotonic sequence | Every change to an entity/table consumes a sequence | Requires an authoritative allocator; replay must preserve allocation | Adds coordination and still needs object-specific predecessors |
| Commit LSN/SCN plus sequence | Native commit identity plus an event/mutation discriminator | Stable within a certified source incarnation; does not directly reveal prior version | Useful basis for provenance and deterministic tokens |
| CDC-maintained object version | Revision of a projected object, maintained by the ordered projector | Stable with deterministic projection, persisted state, and replay discipline | Recommended for this repository |

Recommended version scheme:

```text
version = {
  projection_generation,
  producer_transaction_id,
  mutation_sequence
}
```

The object identity accompanies the token. A token means:

> This projected object reached this revision through this mutation of this source transaction.

Use these fields distinctly:

| Field | Meaning |
|---|---|
| `source_row_before_version` / `source_row_after_version` | Optional application/native row versions, if genuinely available |
| `before_object_version` | Revision in the projector’s planned state before this transaction |
| `expected_destination_version` | Exact destination revision required to apply the mutation |
| `new_object_version` | Deterministically assigned resulting revision |
| `current_object_version` | Revision read from the destination ledger at validation time |

For ordinary replication mutations, `before_object_version` and `expected_destination_version` are equal. `current_object_version` is observed at the destination; a producer cannot authoritatively supply it.

The projector’s state is **planned state**, which may be ahead of destination application.

Version comparison uses exact equality. A numerically “greater” transaction sequence does not make a failed expected-version check acceptable.

Distinguish three object states:

- `PRESENT(version)`
- `DELETED(version)` — retained tombstone evidence
- `ABSENT(baseline)` — confirmed never present in the initialized generation

`UNKNOWN` is not ABSENT and cannot satisfy a normal mutation.

Deletion advances the object version and retains its ledger entry. Re-creation expects the deletion version. This prevents delayed events from resurrecting deleted state.

One source planner must own each destination object namespace. Multiple independent PostgreSQL/Oracle sources writing the same object require a shared ordering/conflict authority and are outside the recommended first version.

## 7. Event and control-topic schemas

A dedicated application control topic should exist:

```text
cdc.tx.control.v1
cdc.tx.mutations.v1
```

Keep it separate from Kafka’s internal transaction machinery and from raw Debezium metadata.

Both topics carry immutable records. Partitioning by canonical transaction ID is recommended for locality, but correctness must tolerate independent arrival, duplicate delivery, and MM2 offset changes.

Use a manifest header plus numbered manifest pages, so a large transaction does not require one oversized Kafka message.

A conceptual header:

```json
{
  "protocol_version": 1,
  "message_type": "MANIFEST_HEADER",
  "transaction_id": "canonical-source-transaction-id",
  "source_database_id": "legacy-postgres",
  "source_incarnation": "incarnation-1",
  "capture_scope_id": "inventory-v1",
  "native_transaction_id": "9001",
  "source_status": "COMMITTED",
  "source_commit_position": {
    "kind": "adapter-verified-position-type",
    "value": "lossless-native-position"
  },
  "source_commit_sequence": "103",
  "projection_generation": "inventory-projection-v1",
  "projection_version": "1",
  "schema_version": "1",
  "correlation_id": null,
  "captured_source_event_count": 3,
  "source_collection_counts": {
    "inventory.customers": 1,
    "inventory.accounts": 1,
    "inventory.orders_flat": 1
  },
  "expected_mutation_count": 7,
  "manifest_page_count": 1,
  "required_destination_domains": ["inventory"],
  "manifest_hash": "sha256:...",
  "mutation_set_hash": "sha256:..."
}
```

Each manifest page contains:

- Transaction identity and generation.
- Page number and page hash.
- Mutation descriptors.
- Declared dependencies and preconditions associated with those descriptors.

A mutation descriptor contains:

```json
{
  "mutation_sequence": 1,
  "mutation_id": "sha256:...",
  "entity": "client.customers",
  "primary_key": {
    "id": {"type": "int64", "value": "1001"}
  },
  "operation": "UPDATE",
  "source_event_ids": ["source-event-identity"],
  "source_record_sequences": [1],
  "before_object_version": {
    "generation": "inventory-projection-v1",
    "producer_transaction_id": "TX100",
    "mutation_sequence": 1
  },
  "expected_destination_version": {
    "generation": "inventory-projection-v1",
    "producer_transaction_id": "TX100",
    "mutation_sequence": 1
  },
  "new_object_version": {
    "generation": "inventory-projection-v1",
    "producer_transaction_id": "TX103",
    "mutation_sequence": 1
  },
  "payload_hash": "sha256:..."
}
```

The corresponding mutation-topic record contains the same identity, descriptor hash, schema information, source positions, and the actual before/after projected payload. DELETE has no resulting row payload.

Additional rules:

- Operations are `INSERT`, `UPDATE`, or `DELETE`; snapshot loading uses a separate bootstrap protocol.
- Canonical keys include typed values and stable field ordering. Delimiter concatenation is insufficient.
- Mutation IDs derive from generation, canonical transaction identity, object identity, and deterministic mutation sequence.
- Correlation IDs are optional business metadata and do not participate in deduplication.
- Hashes cover canonical immutable content, excluding arrival timestamps and transport offsets.
- A manifest binds both the expected mutations and their contents.
- Identical IDs with different hashes are protocol conflicts.
- A zero-mutation transaction has zero pages/mutations as specified by the schema and can still be recorded as applied.
- `source_status=COMMITTED` does not mean the destination has applied it.

Optional status and DLQ topics can be published from a database outbox:

```text
cdc.tx.status.v1
cdc.tx.dlq.v1
```

They are notifications. Database state remains authoritative.

Authoritative capture, manifest, and mutation logs must preserve required history. Upsert topics keyed only by business key are unsuitable for transaction reconstruction if intermediate records can disappear. Flink also offers optional buffering that retains only the latest record for a key. [Flink upsert connector behavior](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/upsert-kafka/)

## 8. State machine and incomplete transactions

Maintain source assembly state separately from destination application state.

Destination lifecycle:

```text
COLLECTING
  → COMPLETE
  → WAITING_DEPENDENCIES
  → READY
  → APPLYING
  → APPLIED

COLLECTING → INCOMPLETE
INCOMPLETE → COLLECTING → COMPLETE       valid late fragments

APPLYING → RETRYABLE → READY             after dependency revalidation

Any eligible nonterminal state → FAILED → DLQ
```

Meanings:

| State | Meaning |
|---|---|
| `COLLECTING` | Header, pages, or mutation payloads are missing. |
| `COMPLETE` | All required artifacts exist and identities, counts, sequences, and hashes validate. |
| `WAITING_DEPENDENCIES` | Complete, but required predecessors or preconditions are unsatisfied. |
| `READY` | All declared prerequisites are satisfied at the last check. |
| `APPLYING` | A worker is executing the database transaction. |
| `APPLIED` | Business changes, versions, and the application receipt committed together. |
| `INCOMPLETE` | Assembly deadline expired; no partial business application is permitted. |
| `RETRYABLE` | A transient attempt failed and left no partial business changes. |
| `FAILED` | A deterministic error requires repair or investigation. |
| `DLQ` | Durably quarantined; automatic application is disabled. |

Suggested initial timeout policy, subject to approval and measured latency:

- Warn after two minutes without assembly progress.
- Mark INCOMPLETE after ten minutes from first fragment.
- Quarantine after 24 hours unresolved.
- Do not extend deadlines merely because duplicates arrive.
- Base timers on capture/arrival progress, not the original source transaction’s start time.
- Accept valid late fragments automatically while INCOMPLETE.
- After DLQ, retain late fragments but require an audited requeue decision.

Missing BEGIN, END, source records, manifest pages, or mutation payloads cannot be repaired by guessing a count.

At the source planner, an unresolved earlier transaction prevents advancing its planning frontier. At the destination, unrelated transactions with complete manifests and satisfied dependencies may continue.

## 9. Dependency rules and DAG

For a mutation of object `X`:

```text
TX2 expects X@V
TX1 produced X@V
⇒ TX1 → TX2
```

This is the basic write/write predecessor rule.

Only the object version visible at the end of a source transaction is externally produced under the recommended net-mutation model. Internal source operations do not create transaction self-edges.

Dependencies need explicit types:

| Type | Satisfaction rule |
|---|---|
| `WRITE_PREDECESSOR` | Producer transaction applied; destination object matches the expected version when locked. |
| `AFTER_TRANSACTION` | Named transaction has a valid applied receipt in the required domain/generation. |
| `VERSION_PRODUCED` | Durable history proves the specified revision was successfully produced. |
| `CURRENT_VERSION_EQUALS` | Object currently has exactly the declared revision during the apply transaction. |
| `EXISTS` / `NOT_EXISTS` | Required object presence is validated under the relevant lock/constraint. |
| Business dependency | A named, versioned rule defines its exact predicate and lock scope. |

`VERSION_PRODUCED` and `CURRENT_VERSION_EQUALS` are different. The first can remain true after the object advances; the second cannot.

Example:

```text
TX100 produces Customer/42@V8 ─┐
                               ├→ TX103
TX101 produces Product/P7@V3 ──┘

TX102 modifies Customer/99 independently
```

TX103 can proceed only after its required versions/dependencies validate. TX102 need not wait.

Edges are discovered from:

1. Expected object-version tokens.
2. Explicit transaction/version dependencies.
3. Approved FK and business dependency rules.
4. Read-preservation constraints, where supported.

Persist unresolved producer references even if the producer’s manifest has not arrived yet. Kafka arrival order must not determine graph structure.

A transaction is READY only when complete, compatible with the active baseline/schema, past its retry delay, and free of unresolved dependencies or conflicts.

**Read dependencies**

CDC normally exposes writes, not the source transaction’s read set. The write/write rule therefore cannot establish full serializability.

An application can publish explicit read dependencies through an outbox or transaction-context record committed with its business changes. However, a source MVCC read of `X@V` does not necessarily mean X was still at V when that source transaction committed.

For the first version:

- Support explicit “after this transaction/version was produced” dependencies.
- Support exact-current predicates only when they agree with the source planner’s current state at that transaction’s sequence.
- Reject arbitrary historical exact-read expectations.
- For accepted exact-current readers, add an edge from the reader to the next writer that would replace that version. Otherwise that writer could overtake the reader.
- Do not claim to reproduce unobserved source reads.

These declarations must be available before the source planner seals the relevant later transactions.

**Foreign keys and lifecycle**

Parent creation dependencies are only part of the problem. Parent deletion can race with child insertion.

The apply algorithm must lock relevant referenced objects and enforce actual database constraints. Parent-before-child inserts and child-before-parent deletes are ordered within the destination transaction. Deferred constraints may be used where the approved final-state semantics require them.

A table relationship cycle is not necessarily a transaction dependency cycle. Valid mutually related changes can belong to one atomic transaction.

**Cycle detection**

Ordinary write-predecessor edges should point from lower to higher source commit sequence. A cycle indicates inconsistent version assignment, bad explicit dependencies, invalid projection rules, or incompatible source ownership.

Check proposed edges for cycles and run periodic strongly connected component checks over unresolved work. Quarantine the affected transactions with an explanation. Never remove an edge merely to make the graph executable.

A graph endpoint/export should report nodes, typed edges, required versions, current versions, blockers, and an `as_of` timestamp from a consistent database snapshot.

## 10. Scheduler placement and concurrency

| Placement | Assessment |
|---|---|
| Flink-only scheduler | Good for stream state; awkward as the authority for external database commits and worker races. |
| In-memory Java scheduler | Simple initially, but loses authoritative state on restart and duplicates coordination work. |
| PostgreSQL-only implementation | Strong transactional foundation, but projection decoding and operational logic become cumbersome in stored procedures. |
| Java engine + PostgreSQL state | Recommended: Java handles protocol/application logic; PostgreSQL owns durability, readiness, locking, and receipts. |

Use a shared Java core for local and AWS execution. Spring is optional.

For minimal deployment expansion:

- Local mode runs ingestion and apply loops in a Java process.
- AWS Lambda stages Kafka deliveries durably.
- Apply workers use the same core, with event-driven draining plus a scheduled sweep so retries and waiting work progress without new Kafka events.
- Database transaction limits must fit comfortably inside the Lambda execution budget. The current 30-second Lambda timeout needs explicit sizing.
- If measured transaction sizes make bounded Lambda execution unsuitable, use a long-running Java worker without changing the protocol.

Concurrency model:

1. Select an eligible transaction using `FOR UPDATE SKIP LOCKED`.
2. Keep ownership of that transaction row until the apply transaction finishes.
3. Create or locate persistent object-state rows, including for absent objects.
4. Lock the write set and required read/FK objects in canonical order.
5. Revalidate dependencies and exact versions.
6. Apply and commit.

`SKIP LOCKED` is appropriate for queue consumers, while object locking and atomic validation establish correctness. [PostgreSQL locking behavior](https://www.postgresql.org/docs/16/sql-select.html)

Avoid leases in the first version. A database transaction lock provides ownership and is released on rollback or connection loss.

Under this model, APPLYING is an in-flight state: other sessions may still see the last committed READY state while the row is locked. A separate attempt/heartbeat record can support monitoring, but cannot authorize a second worker to bypass the lock.

Prevent starvation through oldest-ready-first selection, bounded worker transactions, retry backoff, periodic sweeps, and alerts for old work. A transaction with genuinely missing dependencies remains blocked; aging must not override correctness.

## 11. Destination transaction algorithm

Kafka ingestion and business application are separate durable steps.

First, ingest each message into the inbox. Advance Kafka acknowledgment only past records durably staged or durably quarantined. Use explicit per-partition safe offsets in the local consumer; Lambda must fail ingestion if durable staging fails.

Then execute:

```text
BEGIN DATABASE TRANSACTION

  Lock the queued transaction.

  Check applied receipt:
    same transaction + same manifest hash → duplicate success
    same transaction + different manifest hash → protocol conflict

  Recheck complete, sealed manifest and mutation coverage.

  Lock all affected object-state rows and required predicate objects.

  Revalidate dependencies, baseline, schemas, presence,
  and exact expected destination versions.

  Apply every INSERT / UPDATE / DELETE.

  Update every affected object version, including deletion tombstones.

  Insert the immutable applied-transaction receipt.
  Mark transaction APPLIED.
  Insert status/outbox notifications.

COMMIT
```

Additional rules:

- INSERT requires the declared absent/deleted predecessor state.
- UPDATE and DELETE require the declared existing state and version.
- Unexpected affected-row counts are errors.
- A version mismatch is never silently treated as success.
- Do not fall back to unconditional upsert.
- Do not catch an FK error and commit the remainder.
- Business writes, object versions, and the APPLIED receipt share one connection and one transaction.
- Destination writes outside this protocol are prohibited for owned objects, or must use the same version/locking contract.
- External side effects go through an outbox; they do not run inside the database transaction.

If the connection is lost around COMMIT, treat the outcome as unknown. Reconnect and inspect the applied receipt before retrying. The receipt and business state cannot disagree when both were committed atomically.

## 12. Metadata tables, keys, and indexes

All apply-control tables below live in the same database as their protected business state.

Let `application_key = (projection_generation, destination_domain, transaction_id)`.

| Table | Principal key/content | Important indexes or constraints |
|---|---|---|
| `cdc.source_registry` | Source identity, incarnation, capture scope, adapter version, ownership | Unique active owner per object namespace |
| `cdc.baseline` | Generation, source cut, snapshot identity, completion status | One active baseline per domain |
| `cdc.inbox_message` | Stable message ID, content hash, payload, transport provenance | Unique message ID; transport identity includes cluster/topic incarnation |
| `cdc.transaction` | Application key, manifest hash, source sequence, state, counts, timestamps, next attempt | Partial READY/RETRYABLE index by eligibility time; source-sequence index |
| `cdc.manifest_page` | Application key + page number, page hash/content | Unique page number |
| `cdc.mutation` | Application key + mutation sequence, mutation ID, object, operation, versions, payload hash | Unique mutation ID; object/version lookup |
| `cdc.dependency` | Dependent application, predecessor/version, dependency type/reason | Unresolved-by-dependent and dependents-by-predecessor indexes |
| `cdc.object_state` | Domain/generation/entity/canonical key, presence, current version, last transaction | Unique object identity; row also serves as the stable lock target |
| `cdc.version_history` | Object identity + version, producer application, presence | Unique producer/version mapping |
| `cdc.applied_transaction` | Application key, manifest hash, counts, committed timestamp | Unique application key; immutable |
| `cdc.apply_attempt` | Attempt identity, transaction, worker, outcome, SQLSTATE/error | Transaction/time and unresolved-attempt indexes |
| `cdc.outbox` | Event ID, transaction, payload, publication state | Partial unpublished index |
| `cdc.quarantine` | Transaction/message identity, reason, evidence, requeue audit | Open incidents by age/reason |

Dependency rows must allow unresolved references: requiring the predecessor’s transaction row to exist immediately would make ingestion depend on arrival order.

Source-side Flink state separately retains contribution mappings, normalized transaction identity mappings, planned object versions, and the contiguous planning frontier.

## 13. Cross-database behavior

There are two possible contracts.

| Contract | Guarantee |
|---|---|
| One logical database containing both schemas | Entire captured/projected source transaction commits atomically |
| Existing two databases | Atomic participant commits; recoverable, observable partial delivery |

I recommend the first for the requested invariants. Two schemas in one logical database are sufficient; two logical databases on one cluster are still separate transaction boundaries.

If the split topology is retained:

- The manifest lists all required participants.
- Each participant commits its local mutations, version changes, receipt, and outbox atomically.
- A coordinator verifies matching receipts from all required participants.
- Aggregate status includes `PARTIALLY_APPLIED`.
- Aggregate APPLIED is permitted only after every required receipt exists.
- Failure of participant B does not undo participant A’s committed visibility.
- Exact cross-database current-version predicates are unsupported without additional coordination.
- Compensation requires explicit business actions; it is not equivalent to rollback.
- Successors requiring the whole source transaction wait for aggregate completion.

A global completion flag does not hide partially visible rows from ordinary readers.

Prepared transactions/2PC and application-wide visibility indirection are alternatives, but introduce substantial recovery and reader-coordination requirements. They are not recommended for this first version.

## 14. Recovery, replay, retention, and schema evolution

| Failure | Required behavior |
|---|---|
| Flink crashes before a checkpoint completes | Restore source progress and all assembly/projection/version state consistently; replay input |
| Same source transaction is emitted again | Reproduce identical canonical identity, versions, and content |
| MM2 delivers manifests before payloads | Remain COLLECTING |
| Duplicate fragment arrives | Compare hash; accept identical content without incrementing distinct counts |
| Conflicting duplicate arrives | Quarantine; never overwrite the original silently |
| Worker crashes before DB COMMIT | Business changes and version updates roll back |
| Worker crashes after COMMIT | Applied receipt makes retry a no-op |
| Status publication fails | Outbox retries; database receipt remains authoritative |
| Required predecessor is missing | WAITING_DEPENDENCIES, with an explicit blocker |
| Current version conflicts with expected | Investigate lineage/drift; no blind retry or overwrite |
| Source history/checkpoint required for recovery is lost | Stop the affected generation and reconcile or rebuild |

Retry transient database/network errors with exponential backoff and jitter. Recheck dependencies before another attempt. Retry an entire destination transaction, not only its failed mutation.

DLQ means durable quarantine with transaction identity, manifest hash, missing/conflicting fragment information, error classification, and repair history. It must never satisfy a dependency.

Bootstrap needs a separate protocol:

1. Establish a consistent source snapshot and streaming cut.
2. Build both projected business state and contribution/version state.
3. Assign deterministic baseline versions.
4. Validate snapshot completion and reconciliation.
5. Activate the baseline.
6. Apply queued post-cut transactions.

A snapshot row does not reconstruct the original transaction that created it. Existing destination rows cannot acquire trustworthy predecessor versions from `updated_at` or `source_tx_total_order`.

Use a controlled read gate or a shadow-generation cutover during initialization. UNKNOWN state remains blocked until baseline activation. A verified connector completion signal is required; an arbitrary quiet period is insufficient.

For replay:

- Same generation and same manifest: idempotent replay.
- Changed projection rules: new generation or explicit migration.
- Rewinding Kafka offsets alone does not rewind the destination.
- Recovery of the source planner requires a compatible checkpoint and retained input, not just destination receipts.

For retention:

- Start with at least seven days of hot capture/control/mutation history, then size it against the actual outage/replay objective.
- Retain incomplete, failed, and quarantined evidence until resolved or explicitly abandoned.
- Retain applied identities, current versions, and deletion tombstones until a formally advanced replay floor makes cleanup safe.
- Do not automatically TTL correctness state in version 1.
- Delete payload history only when application, recovery, replay, and outbox requirements permit it.
- Record the supported replay floor and reject older unproven replays.

For schema evolution:

- Version protocol, payload schema, capture scope, projection rules, and generation separately.
- Additive compatible fields may remain within the protocol major version.
- Changes to keys, version meaning, mutation identity, or dependency semantics require a migration boundary.
- Unknown mandatory fields/versions block application.
- Never change capture filters or projection behavior halfway through a transaction.
- DDL, TRUNCATE, and unsupported operations must be handled explicitly or quarantined; they must not be silently ignored.

Observability should expose transaction assembly age, missing sequence ranges, frontier stalls, expected/received counts, unresolved edges, oldest READY age, lock contention, retries, version conflicts, apply latency, outbox lag, replay floor, and aggregate participant status where applicable.

Logs should carry canonical transaction ID, source identity, generation, manifest hash, worker attempt, and the relevant object identity.

## 15. Required repository changes after approval

These are proposed changes only.

| Module/file | Required work |
|---|---|
| `local/source/connectors/legacy-postgres-source-config.json` and AWS connector block in `user_data.sh.tpl` | Ordered capture routing, source identity contract, metadata preservation, explicit producer settings |
| `local/source/flink/sql/init.sql` and corresponding AWS SQL | Replace the authoritative independent fan-out path with transaction-aware assembly/projection; correct null/delete/key-change behavior |
| New `transaction-protocol/` module | Versioned schemas, canonical identities/hashes, validation, source adapters |
| New `transaction-projector/` Flink module | Assembly, source ordering frontier, contribution state, deterministic versions/manifests, checkpoint recovery |
| `local/source/docker-compose.yml`, `user_data.sh.tpl` | Deploy new Flink job/artifact and durable checkpoint storage; explicit topic provisioning |
| `local/source/mm2.properties`, `variables.tf`, AWS MM2 block | Replicate control/mutation topics; explicit retention and configuration management |
| New shared `transaction-apply/` module | Durable inbox, completeness validation, DAG/queue logic, atomic application, retries/outbox |
| `lambda_java/.../Handler.java` | Staging-oriented ingestion, correct failure signaling, shared apply core/worker entry points |
| `local/cloud/lambda-consumer/.../LocalLambdaConsumer.java` | Shared core and safe acknowledgment positions |
| Client/operational SQL initialization and embedded Java DDL | Versioned migrations for control tables and selected atomicity topology |
| `lambda.tf` | New subscriptions, worker scheduling, bounded concurrency/time budgets, permissions |
| `aurora.tf`, `local/cloud/docker-compose.yml`, connection configuration | Consolidated logical database if strict mode is approved |
| `local_deployment.tf` | Track new modules, artifacts, migrations, and deployment inputs |
| Java tests, `local/tests/*`, local/AWS E2E scripts | Contract, real-DB, concurrency, and fault-injection coverage |
| README/system documentation and operations scripts | Protocol, recovery, DAG inspection, quarantine/requeue, retention procedures |

The legacy single-stack demo should be clearly identified as a different flow unless it is explicitly brought into the new protocol.

## 16. Alternatives rejected for version 1

- **Use `source_tx_total_order` as a version:** it restarts per transaction.
- **Use Kafka offsets or timestamps as object versions:** they do not establish object predecessor identity.
- **Group by the full current Debezium PostgreSQL transaction ID:** the pinned connector changes its LSN suffix.
- **Publish one END marker and trust arrival order:** it proves neither source nor transformed-data completeness.
- **Count raw CDC records as destination mutations:** fan-out, filtering, and net projection invalidate the count.
- **Build versions by sorting only transactions already received:** a delayed earlier transaction can be missing.
- **Apply records and repair FK failures afterward:** exposes partial source transactions.
- **Use compacted business topics as the transaction archive:** required intermediate evidence may disappear.
- **Use a graph database:** PostgreSQL adjacency/state tables are sufficient at the current scale.
- **Use distributed leases before necessary:** database locks provide simpler worker ownership.
- **Claim end-to-end exactly-once from Flink/MM2 settings:** destination commit and recovery remain separate concerns.
- **Infer full source read dependencies from CDC:** the required information is absent.
- **Adopt 2PC by default:** substantially expands failure recovery and operational scope.

## 17. Explicit implementation invariants and acceptance evidence

| Invariant | Required test evidence |
|---|---|
| APPLIED implies every declared mutation and version committed in the same destination transaction | Real PostgreSQL observer and rollback tests |
| Replay of the same transaction/hash produces no second business effect | Replay before/after worker restart and lost acknowledgment |
| Same identity with different content never deduplicates successfully | Conflicting header/page/mutation tests |
| UPDATE/DELETE cannot apply against a different expected version | Stale, future, UNKNOWN, and tombstone cases |
| READY requires a complete manifest and satisfied declared prerequisites | Randomized arrival order, missing pages, missing predecessors |
| Missing fragments cannot produce success | Missing BEGIN/END/row/page/payload fault cases |
| Raw counts and projected counts are validated independently | Fan-out, filtering, and zero-mutation transactions |
| An apply failure leaves no partial business or version changes | Failure after each mutation and immediately before COMMIT |
| Independent READY transactions can execute concurrently | Two-worker barrier test on disjoint objects |
| Conflicting transactions cannot both apply from the same predecessor | Deterministic concurrent-worker test |
| A worker crash cannot permit overlapping ownership | Connection-loss and locked-transaction tests |
| Kafka acknowledgment never passes undurable input | Crash during a multi-record poll batch |
| Source version assignment cannot pass an incomplete earlier transaction | Delayed earlier transaction/frontier tests |
| Restart preserves source transaction identity and version assignment | Pinned Debezium/Flink restart and replay fixtures |
| DELETE/re-create retains a valid version chain | Late update, duplicate delete, and reinsert tests |
| Null updates, target-key changes, and contribution removal are correct | Real projection fixtures for accounts/products |
| Explicit exact-current reads cannot be overtaken by the next writer | Reader/next-writer scheduling tests |
| Dependency cycles cannot be silently bypassed | Deliberate cycle and invalid forward-reference cases |
| Bootstrap does not treat uninitialized state as absent | Partial snapshot, interrupted bootstrap, and cutover tests |
| Schema/capture changes cannot silently alter an existing transaction | Compatibility and mixed-version rejection tests |
| Strict atomicity claims stop at the selected database boundary | One-DB atomicity tests; explicit partial-delivery tests if split mode is chosen |
| Cleanup cannot invalidate the supported replay contract | Replay-floor and retention boundary tests |

The first implementation milestone should certify the pinned connector’s BEGIN/data/END identity, counts, source ordering, restart behavior, and snapshot completion. Those fixtures are the foundation for the rest of the protocol.

## DECISIONS REQUIRING APPROVAL

1. **Atomicity boundary:** Approve the recommended single logical destination database with `client` and `operational` schemas, or retain two databases and explicitly accept participant-level atomicity plus `PARTIALLY_APPLIED`.

2. **Source ordering:** Approve one ordered capture topic/planning lane per source for version 1, including blocking the planning frontier behind incomplete earlier transactions.

3. **Version semantics:** Approve CDC-maintained projected-object revision tokens, exact expected-version matching, durable tombstones, and a separate source commit sequence.

4. **Ownership:** Approve one source owner per destination object namespace. Shared multi-source writes require a separate conflict/ordering design.

5. **Projection semantics:** Approve one net mutation per object per transaction. Confirm rules for conflicting account/product contributions; recommended default is quarantine rather than arbitrary overwrite.

6. **Dependency scope:** Approve write predecessors, explicit applied/history dependencies, FK/business predicates, and restricted exact-current reads. Unobserved or arbitrary historical source reads remain outside the guarantee.

7. **Incomplete transactions:** Approve warning/INCOMPLETE/quarantine thresholds of 2 minutes/10 minutes/24 hours as initial settings, automatic recovery before DLQ, and audited requeue afterward.

8. **Bootstrap and cutover:** Approve a consistent baseline with contribution/version state and controlled visibility. Existing destination rows require reconciliation or a new generation.

9. **Runtime:** Approve the hybrid Flink projector plus shared Java/PostgreSQL apply engine, initially reusing local Java and AWS Lambda entry points with a scheduled retry sweep.

10. **Operational contract:** Approve explicit transaction/message size limits, retained replay evidence, and no automatic deletion of correctness metadata until a replay floor is established.

11. **Connector scope:** Approve PostgreSQL-first implementation and certification. Oracle remains a separate adapter and validation phase under the same protocol.

**Approval record:** Request 2 explicitly proceeded with the recommended defaults above. Implementation details and remaining boundaries are recorded in `docs/transaction-protocol-operations.md`.
