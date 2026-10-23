# Transaction-Preserving CDC Distributed-Systems Correctness Review

Date: 2026-10-07

Scope: repository implementation after Requests 1–3

Review type: distributed-systems correctness, failure windows, and invariant enforcement

AWS execution: **not performed**

## Executive verdict

**The local destination apply core has a sound atomic-commit and multi-worker locking foundation, but the complete pipeline is not safe enough to proceed to AWS validation.**

The destination path correctly places business mutations, object-version transitions, the applied receipt, attempt record, and outbox record in one PostgreSQL transaction. Durable inbox staging precedes Kafka acknowledgement. `FOR UPDATE SKIP LOCKED`, canonical object locking, exact-version comparisons, conditional SQL, and the unique applied-transaction key prevent the most important duplicate-worker and partial-commit failures.

Release remains blocked by protocol-critical source and bootstrap defects:

1. Replaying a completed source transaction does not reproduce or suppress the same immutable output.
2. Transaction identity is based on a reusable PostgreSQL XID without the approved verified boundary discriminator.
3. The mutation-set/manifest hash does not bind the complete mutation descriptor.
4. Unknown objects are synthesized as `ABSENT(BASELINE)` by both the projector and apply service without an activated, reconciled baseline.
5. The approved contribution model for many-to-one projections is absent.
6. AWS Flink recovery is not production-safe: the checkpoint directory key is incorrect, storage is host-local, and no automatic HA/savepoint restore procedure is configured.

The implementation therefore supports this narrower statement only:

> At-least-once transport, durable idempotent staging, and atomic/replay-safe destination effects for a valid immutable manifest within one initialized PostgreSQL database—subject to the known source replay, manifest-integrity, and baseline limitations.

It must not be described as end-to-end exactly once.

## Review scope and evidence

The review inspected:

- the approved design in `docs/design/transaction-protocol-design.md`;
- shared protocol models and hashing in `transaction-protocol/`;
- Debezium routing, Flink assembly, checkpoint state, planning, and Kafka sink behavior;
- MirrorMaker 2 topic replication;
- durable inbox, transaction/DAG state, object locks, SQL mutations, receipts, retries, outbox, and quarantine;
- local consumer Kafka acknowledgement behavior;
- Lambda MSK batch failure behavior and scheduled retry sweep;
- local and AWS configuration, migrations, and existing Request 3 evidence.

Fresh test evidence on the final reviewed code:

| Command/suite | Result | Interpretation |
|---|---:|---|
| `transaction-apply: gradle test --rerun-tasks --no-daemon` | **26 passed, 1 failed** | The only failure is the retained red test `manifestMustBindResultingVersionAndDependencies`. |
| `transaction-projector: gradle test --rerun-tasks --no-daemon` | **12 passed, 1 failed** | The only failure is the retained red test `transactionReplayMustNotGenerateNewVersionsOrHashes`. |
| Focused destination regressions | **3 passed** | Missing WRITE version is quarantined; unrelated ancestors survive cycle quarantine; a cycle missed by concurrent initial checks is found by the sweep. |

The prior real local E2E and destination replay evidence is retained in `docs/transaction-protocol-request3-validation.md`; it was reviewed but not rerun. No AWS resources were changed or exercised.

## Critical correctness issues

### C1. Source replay changes transaction output

`TransactionProjectorFunction` removes a completed assembly after emission. `ProjectorState` retains advanced object state but no immutable completed-transaction assignment or output evidence. Replaying the raw transaction therefore plans it against its own already-advanced versions. In addition, manifest content includes processing-time and transport-derived values (`firstSeenEpochMs`, deadline, and END Kafka offset).

The retained red test reproduces this directly. A Kafka rewind, checkpoint loss, or manual resubmission from `earliest` can produce the same logical source transaction with different expected/new versions or hashes. Destination conflict handling prevents silent double application, but the result is quarantine/stall rather than replay-safe recovery.

Required resolution: persist the canonical transaction occurrence, stable commit-sequence assignment, sealed descriptor set, and replay evidence; exclude arrival time and transport offsets from immutable identity/hashes; define a migration or new projection generation.

### C2. Canonical transaction identity can collide after PostgreSQL XID reuse

`ProtocolCodec.canonicalTransactionId` is only:

```text
source_database_id/source_incarnation/capture_scope_id/native_xid
```

The projector deliberately strips Debezium’s suffix from the transaction ID and does not add the approved verified commit-boundary discriminator. PostgreSQL XIDs are not globally unique for the life of a source incarnation. A sufficiently old replay or long-lived source can collide with a historical receipt and be interpreted as either a duplicate or conflicting content under the same transaction ID.

This is inseparable from C1 and requires a versioned identity transition, not a local patch to existing v1 history.

### C3. Manifest integrity omits versions, dependencies, operation, and object identity

`ProtocolCodec.mutationSetHash` currently hashes only mutation sequence, mutation ID, and the claimed payload hash. The destination recomputes that same incomplete hash. Changing `newObjectVersion` or a dependency leaves the set hash unchanged; the red test proves it.

Some tampering is now rejected by semantic validation, but semantic checks are not a substitute for an immutable manifest commitment. Valid-looking changes to dependency type, referenced object, expected version, operation, source lineage, or key may not be cryptographically bound by the manifest.

Required resolution: define a canonical mutation descriptor and version the wire format/projection generation before changing hashes already represented in durable state.

### C4. UNKNOWN is silently promoted to ABSENT baseline for normal writes

The approved protocol states that `UNKNOWN` is not `ABSENT`. The projector uses `getOrDefault(objectKey, absentBaseline)` for every previously unseen object. The apply service then creates a missing `object_state` row whenever a mutation claims producer `BASELINE`.

This lets an unauthenticated producer assertion manufacture proof of absence. With a pre-existing destination row, the business insert fails and rolls back; with no row, it applies even though no baseline activation proves absence. This breaks the bootstrap/cutover invariant and can assign an invalid version lineage.

Required resolution: implement source registry, baseline state, snapshot completion/reconciliation, generation activation, and controlled transition from UNKNOWN. Snapshot rows cannot serve as ordinary single-row source transactions.

### C5. Approved contribution/net-projection semantics are not implemented

The planner stores only object versions and last payloads. It has no durable source-row/contribution ownership model. It rejects two different same-object contributions within one transaction and treats shared products specially by merely avoiding delete generation. It cannot correctly answer whether a projected object remains contributed by another source row, nor reliably remove an old contribution after a key change or null transition.

Consequences include false conflicts, stale products, and incorrect deletes/updates for many-to-one mappings. This is a state-model gap, not a style issue.

Required resolution: implement the approved deterministic contribution state and one net mutation per object per source transaction, with entity-specific conflict rules.

### C6. AWS Flink state recovery does not satisfy the protocol

The local Compose file uses `state.checkpoints.dir`, but `user_data.sh.tpl` uses `execution.checkpointing.dir`, which is not the checkpoint storage key used by the local corrected configuration. The AWS path is also an EC2 host bind mount and there is no JobManager HA or explicit restore-from-savepoint/checkpoint procedure in the deployment.

An EC2 or JobManager loss can therefore lose assembly, planned versions, sequence assignments, and deduplication state. Restarting the job from `earliest` then reaches C1. This alone prevents AWS validation from being meaningful as a correctness gate.

## High-risk issues

### H1. Dependency vocabulary is incomplete

The approved protocol distinguishes `VERSION_PRODUCED` from `CURRENT_VERSION_EQUALS` and defines restricted exact-current read scheduling/anti-dependencies. The implementation enum supports only `WRITE_PREDECESSOR`, `AFTER_TRANSACTION`, `EXISTS`, and `NOT_EXISTS`. Unobserved reads remain correctly outside the claim, but even declared exact-current reads cannot be represented or protected from the next writer.

### H2. Source/capture/generation ownership is not authoritative

The migration has no `source_registry` or active `baseline` table. The destination validates a few schema/version integers and mutation-local version generations, but does not prove that a source/incarnation/scope owns an object namespace or that the manifest and every mutation share the registered identity and active generation. Mixed capture scopes or an accidentally reused generation can enter durable state.

### H3. No manifest pages or bounded large-transaction contract

The approved header/page design is represented as one manifest with count/hash only. There is no persisted manifest-page table, page completeness check, or enforced transaction/message-size limit. Large source transactions can exceed Kafka/Lambda/Flink/DB limits or produce unbounded state without a protocol-visible rejection.

### H4. Poison records can retry forever

Malformed JSON or a record without a trustworthy transaction identity cannot be converted to a transaction DLQ event. The local consumer throws before committing that offset; Lambda fails the invocation. That is safer than skipping data but creates an indefinite retry loop with no transport-level poison-message quarantine workflow.

### H5. Projector correctness state is one unbounded JSON value

Assemblies, object versions, and object payloads are stored in one keyed `ValueState<String>`. Tombstones and completed object history have no replay-floor cleanup, and an incomplete/DLQ assembly is retained indefinitely. Serialization and checkpoint cost grow with the entire projection namespace. A single corrupt/oversized value affects the whole ordered lane.

### H6. Cycle detection originally had a concurrent staging blind spot

Two staging transactions can each insert one side of a cycle and perform their initial check before the other uncommitted edge is visible under READ COMMITTED. Previously, both could commit in WAITING and remain there forever because readiness sweeps did not rerun cycle detection.

This review fixed the issue by rerunning persisted cycle detection on every WAITING/RETRYABLE sweep. A regression models the post-race committed graph and verifies both members become FAILED while unrelated work is untouched.

### H7. Retry policy can create synchronized retry pressure

Transient SQL states use a fixed five-second delay. There is no exponential backoff, jitter, attempt cap, or error-class-specific policy. A DB outage can synchronize Lambda/scheduled sweeps and create a retry storm. Correctness checks still run on every attempt, but availability and database recovery risk are high.

### H8. Outbox has no publisher in the reviewed implementation

The applied status event is inserted atomically with business state, which is correct, but no publisher/claimer was found to deliver and mark `cdc.outbox` rows. Status-topic consumers can therefore see permanent lag even while the database is correct.

### H9. Schema migration is not an upgrade-safe migration system

`SchemaMigrator` serializes startup with a session advisory lock and runs one `IF NOT EXISTS` script. It does not transactionally record/check the exact migration before use, validate existing column/constraint definitions, or apply versioned upgrades. An older partially compatible schema can survive silently.

### H10. Destination completion does not validate all source completeness evidence

The destination validates mutation count, consecutive mutation sequence, and mutation-set hash. It stores raw captured-event count but not the source per-collection count map and has no BEGIN/END/page evidence. Correctness relies on the projector having validated these facts before emitting the manifest. That trust boundary is acceptable only after projector replay/identity certification, which is currently failing.

## Lower-risk and operational issues

- READY is a cached scheduling hint and can become stale. Apply-time object locks and dependency/version rechecks preserve correctness, but monitoring must not treat READY as a durable promise of immediate success.
- `APPLYING` is updated inside the same uncommitted DB transaction as the claim. Other sessions normally still observe READY while the row lock prevents a second claim. This is correct but can confuse dashboards.
- The readiness sweep locks every eligible WAITING/RETRYABLE transaction selected by one query. At scale it can create long sweep transactions and fairness pressure; batching is advisable.
- Retry attempts do not record an explicit start row before SQL work, so killed attempts can be absent from `apply_attempt` history even though database locks recover safely.
- Destination COLLECTING timeouts depend on source INCOMPLETE/DLQ control messages. There is no independent destination watchdog for a manifest/payload fragment lost beyond retention.
- DLQ/quarantine has no audited requeue implementation. This correctly avoids unsafe automatic recovery but leaves operations incomplete.
- Four stale WAITING transactions from the earlier local fixture remain documented in Request 3. They should be repaired through an approved rebootstrap/requeue procedure, not forced READY.
- DDL/TRUNCATE handling, unknown tables, and schema/capture transitions are not certified end to end.
- Hard-coded entity priority is sufficient for the current fixed schema but is not a general FK dependency engine.
- Observability lacks the complete required metrics for frontier age, missing ranges, lock contention, retry rate, replay floor, outbox lag, and active generation.

## SQL concurrency review

### What is correctly serialized

1. `claim()` locks one READY/RETRYABLE transaction with `FOR UPDATE SKIP LOCKED` and holds that row lock through destination commit or rollback.
2. Object and predicate rows are locked in canonical object-key order.
3. WRITE predecessors now lock and compare the declared exact object version, not merely the producer receipt.
4. Business UPDATE/DELETE statements include `cdc_version = expected` and require exactly one affected row.
5. Object ledger advancement also uses the exact expected version and requires one affected row.
6. Business rows, object states, version history, applied receipt, transaction state, attempt, and outbox all commit in the same DB transaction.
7. The `(projection_generation, transaction_id)` applied-receipt primary key is the final idempotency barrier.

### Races considered

| Race | Result |
|---|---|
| Two workers claim the same transaction | One row lock wins; the other skips it. Tested. |
| Two transactions consume the same predecessor version | Canonical object lock serializes; second exact-version check fails. Tested. |
| Predicate reader versus writer | Predicate/object row remains locked through commit. Tested with NOWAIT barrier. |
| Producer commits while dependent is staged WAITING | Later staging refresh or periodic drain sweep makes it READY; apply rechecks. Tested. |
| Two dependency edges form a cycle concurrently | Initial checks can miss uncommitted edges; fixed by sweep-time cycle detection. Tested with reconstructed post-race state. |
| DB COMMIT succeeds but JDBC reports failure | Failure recorder locks the transaction and checks immutable receipt before classifying. Tested. |
| Worker dies while transaction row/object rows are locked | PostgreSQL releases locks and rolls back all uncommitted work. Tested with child-process termination. |
| Readiness sweep races with apply | `SKIP LOCKED` avoids claiming an owned row; apply-time checks are authoritative. |

No check-then-act race was found in the final destination mutation path that can expose partial committed business state. The critical remaining races are before that path: source identity/replay/baseline and graph/schema trust.

## Crash-window analysis

### Actual Kafka ingestion boundary

The implementation deliberately separates Kafka acknowledgement from business application:

```text
Kafka poll
→ BEGIN inbox DB transaction
→ insert/validate inbox + transaction fragments
→ COMMIT inbox DB transaction
→ Kafka acknowledgement
→ later claim/apply in another DB transaction
```

| Crash point | Result |
|---|---|
| Before poll | Kafka retains the record. |
| After poll, before inbox BEGIN/insert | No offset commit; Kafka redelivers. |
| After inbox insert, before inbox COMMIT | DB rollback/connection close; Kafka redelivers. |
| Immediately after inbox COMMIT, before Kafka ack | Kafka redelivers; inbox content identity makes it a duplicate no-op or conflict quarantine. |
| Immediately after Kafka ack, before apply | Input is already durable in PostgreSQL; restart/scheduled drain can apply it. |
| During a multi-record Lambda batch after some individual stages committed | Invocation fails; MSK retries the batch; already staged messages deduplicate. |
| Malformed record before durable quarantine | Batch/consumer remains stuck and retries; no silent offset advance, but manual poison handling is required. |

For the local consumer, `commitSync(offset + 1)` is called only after `stage()` commits. For Lambda, any exception is rethrown so the event source mapping does not acknowledge the batch.

### Destination application boundary

```text
BEGIN DB transaction
→ lock/claim transaction row (APPLYING is uncommitted)
→ check applied receipt
→ load and validate complete mutation set
→ lock write/predicate objects
→ revalidate dependencies and exact versions
→ apply all business SQL
→ advance all object versions/history
→ insert immutable applied receipt
→ mark APPLIED + attempt + outbox
→ COMMIT
→ best-effort readiness sweep
```

| Crash immediately… | Result |
|---|---|
| Before claim | No state changed; another worker can claim. |
| After claim, before first business SQL | Claim/APPLYING is uncommitted; connection loss rolls it back to prior READY/RETRYABLE. |
| After any business SQL, before version updates | Entire DB transaction rolls back; no partial business visibility. |
| After version updates, before receipt | Business and version rows roll back together. |
| After receipt, before COMMIT | Receipt and all effects roll back together. |
| After DB COMMIT, before method return | Receipt proves success; retry is a no-op. The uncertain-commit handler rechecks it. |
| During post-commit readiness sweep | Producer remains APPLIED; caller may report failure and retry, but receipt prevents a second effect. A scheduled sweep later wakes dependents. |

There is no Kafka acknowledgement after the apply commit in this architecture—the Kafka record was already acknowledged after durable staging. This removes a Kafka/DB dual-write window from business application while retaining at-least-once inbox delivery.

### Flink checkpoint/output boundary

```text
Kafka source record
→ mutate keyed assembly/projection state
→ emit mutation/control records into transactional Kafka sink
→ checkpoint state and source offsets
→ commit Kafka sink transaction
```

| Crash point | Intended Flink behavior | Current weakness |
|---|---|---|
| Before state mutation | Source record replays. | Safe only if replay uses compatible checkpoint state. |
| After state mutation/output, before completed checkpoint | Restore previous state/offset; uncommitted sink transaction is aborted. | Covered by Flink’s checkpoint transaction model, not by an external process-kill E2E here. |
| After completed checkpoint and sink commit | Output is visible once for that checkpoint epoch. | MM2/destination may still redeliver, which inbox handles. |
| JobManager/host loss with recoverable checkpoint | Restore assembly, planned versions, timers, and offsets consistently. | Local checkpoint-restore harness passes. |
| JobManager/host loss without compatible checkpoint | Reconsume from configured `earliest`. | **Unsafe today because C1/C2 make replay output unstable. AWS recovery configuration is insufficient.** |

### MM2 reorder/duplicate window

Control and mutation records are immutable, keyed by transaction ID, but can arrive independently after MM2. Destination state remains COLLECTING until count/sequence/hash validate; duplicates are content-compared. Dependencies reference transaction/object identities rather than mirrored offsets. This tolerates ordinary duplicate delivery and cross-topic reorder.

It does not tolerate source identity reuse, a descriptor not bound by the manifest, retention loss, or a poison record that permanently blocks a partition.

## Approved invariant review

| Invariant | Enforcement mechanism | Test coverage | Weakness / status |
|---|---|---|---|
| APPLIED implies every declared mutation and version committed together | One PostgreSQL transaction contains business SQL, object/version rows, receipt, state, attempt, outbox | Rollback barriers; live atomic observer | **Strong locally**, but only for a valid complete manifest in one DB. |
| Replay of same transaction/hash has no second effect | Inbox identity, receipt PK/hash check, exact versions | Destination replay and lost-ack tests | **Destination passes; end-to-end fails C1/C2.** |
| Same identity with different content never deduplicates | Semantic JSON inbox comparison; manifest/mutation conflict quarantine; duplicate boundary hashes | Conflicting inbox, mutation, source fragment, END tests | Manifest does not bind full descriptor (C3). |
| UPDATE/DELETE requires exact expected version | Object locks, exact JSON version comparison, conditional SQL row count | stale/future/tombstone/concurrent writer tests | Baseline provenance is untrusted (C4). |
| READY requires complete manifest and prerequisites | count, consecutive sequence, set hash, persisted deps; apply-time recheck | interleaving, missing mutation, fan-in | No pages; hash incomplete; cached READY may be stale but safe at apply. |
| Missing fragments never produce success | source BEGIN/END/count/sequence/collection checks; destination count/hash | incomplete and late-fragment tests | Poison/no-identity records need transport quarantine; destination has no independent timeout. |
| Raw and projected counts are independent | source collection counts versus manifest mutation count | fan-out and wrong collection-count tests | Destination does not retain/revalidate per-collection map. |
| Failed apply leaves no partial business/version state | Single DB transaction | failure-after-SQL, killed-worker tests | Strong for supported one-DB entities. |
| Independent READY transactions may run concurrently | SKIP LOCKED plus disjoint canonical object locks | two-worker barrier test | Sweep can be broad; no global serialization found in apply. |
| Conflicting transactions cannot both consume one predecessor | same object lock + exact version | two-writer concurrency test | Strong locally. |
| Worker crash cannot create overlapping ownership | transaction row lock held to commit/rollback | before/after claim, during SQL process-kill tests | APPLYING is intentionally not durably visible. |
| Kafka acknowledgement never passes undurable input | stage DB commit before local commitSync; Lambda throws on failure | offset helper/batch behavior tests; destination replay | No process-kill broker test at each poll record; poison records stall. |
| Source version assignment cannot pass an incomplete earlier committed transaction | earliest END-complete frontier; earlier-END incomplete blocks | corrected begin-vs-END ordering test; incomplete tests | Commit sequence still uses Kafka END offset and is not replay-stable. |
| Restart preserves source transaction identity/version | checkpointed state and Flink checkpoint model | harness checkpoint/late recovery | **Fails full replay; deployment recovery inadequate.** |
| DELETE/re-create preserves tombstone chain | DELETED object state/history and exact next expected version | lifecycle test | Cleanup/replay floor absent; baseline still untrusted. |
| Null updates, key changes, contribution removal are correct | transition emits old delete/new insert for some projections | limited planner tests | **Not established; contribution model absent (C5).** |
| Explicit exact-current reads cannot be overtaken | none | none | **Not implemented (H1).** |
| Dependency cycles cannot be bypassed | recursive SQL cycle detection on stage and every readiness sweep | direct cycle, ancestor isolation, persisted missed-cycle regression | No periodic SCC batching beyond sweeps; supported edge types only. |
| Bootstrap never treats unknown as absent | intended object_state distinction | NOT_EXISTS unknown test | **Fails for ordinary BASELINE writes (C4).** |
| Schema/capture changes cannot alter an existing transaction silently | strict JSON unknown fields; schema/projection integer checks | unknown-schema tests | No active source/generation registry; migration and mixed-identity checks incomplete. |
| Strict atomicity claim stops at selected DB boundary | client/operational are schemas in one PostgreSQL DB | live/rollback tests | Correct locally; no AWS validation. |
| Cleanup cannot invalidate replay contract | no automatic correctness-state cleanup | none | Safe by retention, but unbounded and no declared/enforced replay floor. |

## Corrections made during this review

All corrections preserve the approved protocol and are intentionally narrow:

1. **Commit-order planning:** changed the source frontier from BEGIN/first-seen insertion order to the earliest authenticated END offset. An overlapping transaction that has not committed no longer blocks one that committed earlier; an incomplete transaction with an earlier END still blocks later ENDs.
2. **Conflicting source duplicates:** persist BEGIN/END content hashes and quarantine attributable conflicting duplicate boundaries/fragments instead of crashing/restarting the operator or silently mutating assembly metadata.
3. **Mutation semantic validation:** reject mismatched source/generation fields, invalid resulting-version producer/sequence/presence, and malformed dependency shapes before readiness.
4. **WRITE predecessor enforcement:** lock the declared dependency object and require both the producer receipt and the exact declared object version.
5. **Persisted cycle recovery:** rerun cycle detection during readiness sweeps to catch cycles missed because concurrent staging transactions could not see each other’s uncommitted edges.
6. **Post-commit wake-up:** execute the best-effort dependent readiness sweep after the producer business commit, keeping the correctness transaction’s lock graph bounded; apply-time checks remain authoritative.

## Tests added or strengthened

- `planningFrontierUsesCommitEndOrderRatherThanBeginOrder`
- `conflictingDuplicateFragmentIsQuarantinedWithoutCrashingOperator`
- `conflictingDuplicateEndMetadataIsQuarantined`
- `writePredecessorRequiresTheDeclaredObjectVersion`
- `malformedResultingVersionIsQuarantinedBeforeReadiness`
- `writePredecessorWithoutVersionIsQuarantinedBeforeReadiness`
- `readinessSweepDetectsCycleMissedByConcurrentInitialChecks`

The two deliberately failing regression tests remain in the suite because converting them to green requires approved wire identity/generation changes:

- `transactionReplayMustNotGenerateNewVersionsOrHashes`
- `manifestMustBindResultingVersionAndDependencies`

## Remaining known limitations

- PostgreSQL-only adapter; Oracle transaction identity/SCN behavior is unimplemented and uncertified.
- No durable source sequence registry or immutable completed-transaction replay index.
- No controlled bootstrap/baseline activation or source ownership registry.
- No contribution model for many-to-one projections.
- No manifest paging or enforced transaction-size contract.
- No exact-current/history dependency types or read anti-dependency scheduling.
- No outbox publisher, audited DLQ requeue, or poison-record transport quarantine.
- Fixed retry delay, no jitter/cap, and incomplete retry-storm controls.
- No replay-floor/retention cleanup protocol.
- AWS Flink HA/checkpoint restore and actual Lambda/MSK/RDS crash windows remain unvalidated.

## AWS validation decision

**NO-GO. Do not proceed to AWS validation yet.**

AWS validation would currently test a deployment whose source replay identity, manifest commitment, baseline semantics, and checkpoint recovery are known to violate approved invariants. It could produce a successful happy-path run without establishing recoverability.

Minimum prerequisites for an AWS go-ahead:

1. Approve and implement v2/new-generation canonical transaction identity and stable replay assignments.
2. Approve and implement full canonical descriptor/manifest hashing.
3. Implement controlled baseline/source-registry activation and remove implicit UNKNOWN→ABSENT behavior.
4. Implement or explicitly narrow scope around contribution semantics with tests.
5. Correct AWS checkpoint storage configuration and define automatic restore/HA or an operational savepoint restore gate.
6. Make both retained red tests pass without rewriting applied v1 history.
7. Rerun the complete local E2E/failure matrix, then perform AWS validation as a separate phase.

## Files changed by this review

- `transaction-apply/src/main/java/com/example/cdc/apply/TransactionApplyService.java`
- `transaction-apply/src/test/java/com/example/cdc/apply/AdversarialApplyPostgresTest.java`
- `transaction-projector/src/main/java/com/example/cdc/projector/ProjectorState.java`
- `transaction-projector/src/main/java/com/example/cdc/projector/TransactionProjectorFunction.java`
- `transaction-projector/src/test/java/com/example/cdc/projector/ProjectorFailureTest.java`
- `docs/transaction-protocol-distributed-correctness-review.md`

No Terraform apply, AWS deployment, Kafka offset reset, destructive database cleanup, or production data mutation was performed.
