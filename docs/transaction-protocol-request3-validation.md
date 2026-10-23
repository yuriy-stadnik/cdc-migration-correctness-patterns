# Request 3 — Local CDC correctness validation

Report date: 2026-10-07.

**Verdict: release blocked; validation is not complete.** The latest saved automated reports contain **41 tests: 39 passed, 2 failed, 0 skipped**. A real local E2E transaction and destination Kafka replay succeeded, but source replay stability and manifest integrity still fail required invariants.

This report summarizes work already performed. **No tests were rerun to prepare this report**, as requested. The commands below are instructions for a future manual run, not additional execution evidence.

Related records: [approved design](design/transaction-protocol-design.md), [Request 2 implementation record](transaction-protocol-implementation.md), [operations guide](transaction-protocol-operations.md).

## 1. Work completed

The verification workflow compared the implementation with the approved invariants, reproduced defects before fixing them, and retained failing regression tests for unresolved requirements. Passing helper tests were not treated as proof of complete distributed correctness.

- Started the local source PostgreSQL, Debezium, source Kafka, Flink, MM2, destination Kafka, destination PostgreSQL, and Java apply consumer.
- Added real-PostgreSQL adversarial tests for ordering, fan-in, replay, conflicting workers, predicate locks, rollback, tombstones, and process death.
- Added actual Flink operator-harness tests for keyed state, checkpoint restoration, timers, incomplete transactions, late fragments, planning order, and replay.
- Added payload/schema/manifest-integrity tests and a destination-key regression.
- Added a repeatable local E2E probe: [local/tests/test-transaction-request3.py](../local/tests/test-transaction-request3.py).
- Inspected raw Kafka capture records, replicated offsets, destination business rows, receipts, dependency rows, worker logs, and Flink checkpoint metadata.
- Reset the dedicated destination consumer group's offsets and checked replay against the retained database.
- Preserved earlier failed/stuck test transactions; did not delete volumes, rewrite receipts, force requeue, or hide failures with a clean database reset.
- Did not deploy to AWS, run Terraform apply, commit, or push changes.

The repository has no Position entity. The live equivalent used the existing `inventory.customers`, `inventory.accounts`, and `inventory.orders_flat` source tables: update customer, update account, insert order, all in one source transaction.

## 2. Latest saved automated results

These are the latest recorded results for each suite, not a claim that one monolithic command ran all 41 tests. Test methods can exercise multiple failure points.

| Module / test class | Tests | Passed | Failed | What it establishes |
|---|---:|---:|---:|---|
| `transaction-apply` / `AdversarialApplyPostgresTest` | 17 | 17 | 0 | Real DB scheduling, replay, conflict, lock, lifecycle, and crash scenarios |
| `transaction-apply` / `TransactionApplyServicePostgresTest` | 3 | 3 | 0 | Whole-transaction apply, stale-version rejection, rollback, two-worker application |
| `transaction-apply` / `ProtocolIntegrityPostgresTest` | 3 | 2 | 1 | Tampered payload/schema rejection pass; full descriptor binding fails |
| `transaction-projector` / `AssemblyRulesTest` | 3 | 3 | 0 | Boundaries, contiguous counts, duplicates, serialized assembly state |
| `transaction-projector` / `InventoryProjectionPlannerTest` | 3 | 3 | 0 | Account fan-out, conflicting contributions, correct order key |
| `transaction-projector` / `ProjectorFailureTest` | 5 | 4 | 1 | Frontier, timeout/DLQ, restore, collection counts pass; replay fails |
| `transaction-protocol` / `ProtocolCodecTest` | 4 | 4 | 0 | Typed object keys, transaction-ID normalization, version round trip |
| `local/cloud/lambda-consumer` / `LocalLambdaConsumerTest` | 1 | 1 | 0 | Offset helper advances only the specified durably staged partition |
| `lambda_java` / `HandlerTest` | 2 | 2 | 0 | MSK payload decoding and topic resolution helpers |
| **Total** | **41** | **39** | **2** | **Not an overall pass** |

The apply and projector `gradle test` commands exited **1**, because their release-blocking assertions remain enabled. Protocol, local-consumer, and Lambda helper test commands succeeded. The latter three were explicitly rerun with `--rerun-tasks`; their final XML timestamps are 2026-10-07 11:22–11:23 UTC. Apply/projector results are from 11:19 UTC. The projector fat JAR and local worker Docker image also built successfully.

Evidence locations, relative to the repository root:

| Module | HTML report | Machine-readable results |
|---|---|---|
| Apply | `transaction-apply/build/reports/tests/test/index.html` | `transaction-apply/build/test-results/test/TEST-*.xml` |
| Projector | `transaction-projector/build/reports/tests/test/index.html` | `transaction-projector/build/test-results/test/TEST-*.xml` |
| Protocol | `transaction-protocol/build/reports/tests/test/index.html` | `transaction-protocol/build/test-results/test/TEST-*.xml` |
| Local consumer | `local/cloud/lambda-consumer/build/reports/tests/test/index.html` | `local/cloud/lambda-consumer/build/test-results/test/TEST-*.xml` |
| Lambda helpers | `lambda_java/build/reports/tests/test/index.html` | `lambda_java/build/test-results/test/TEST-*.xml` |

Build reports are generated artifacts and can be overwritten by future runs. The observations below preserve the important results in this document.

## 3. Scenario matrix and test descriptions

PASS below applies to the stated test layer. It does not imply that every case ran through the complete Docker pipeline.

| Scenario | Expected behavior | Actual behavior | Pass/Fail | Evidence |
|---|---|---|---|---|
| Normal atomic source transaction | All seven projected effects become visible together | Live DB snapshots observed only 0 or 7 effects; one receipt contains seven mutations | PASS, bounded live E2E | Python probe; transaction `legacy-postgres/local-1/inventory-v1/761` |
| Interleaved transactions | Reconstruct each transaction without partial application | Staged A1, B1, C1, A2, B2, A3; observed each transaction's row count as either zero or complete | PASS, DB component | `interleavedFragmentsNeverApplyPartialTransactionsAndReplayIsNoOp` |
| Duplicate mutations, whole transactions, completion metadata | No second destination effect | Replays produce no additional application or version history | PASS, destination component | Same interleaving/replay test; original apply test |
| Out-of-order version dependency | Successor waits until producer commits | Future revision remained WAITING; then both applied in dependency order | PASS, DB component | `futureVersionWaitsForPredecessor` |
| Independent parallel work | Two disjoint transactions execute concurrently | Both reached an AFTER INSERT DB barrier concurrently; a third claimant found no claimable work | PASS, deterministic DB concurrency | `independentWorkersOverlapWhileDuplicateClaimSkipsLockedTransaction` |
| Dependency fan-in | TX4 waits for all three predecessors | WAITING after the first two commits; READY after the third | PASS, DB component | `fanInWaitsForEveryPredecessor` |
| Missing transaction fragment | Never silently apply incomplete work | No apply before final mutation; Flink emits INCOMPLETE and later DLQ under controlled time | PASS at tested layers; destination-only loss timeout remains a gap | `missingFragmentWaitsAndValidLateFragmentRecoversBeforeDlq`; Flink timeout tests |
| Late fragment before DLQ | Recover a valid incomplete transaction | Restored Flink state completed exactly once after the missing fragment; destination became READY | PASS, harness and DB | `timeoutCheckpointRestoreAndLateFragmentBeforeDlqRecover`; DB late-fragment test |
| Late fragment / stale marker after DLQ | Remain quarantined until audited requeue | No source manifest; stale INCOMPLETE marker did not reopen destination DLQ | PASS after fixes | `lateFragmentAfterDlqMustNotProduceManifest`; `lateIncompleteMarkerMustNotReopenDlq` |
| SQL failure before commit | Roll back all business and version changes | Deferred FK failure rolled back customer/address writes, versions, and receipt | PASS, real PostgreSQL | `databaseFailureRollsBackEveryBusinessAndVersionEffect` |
| Process death before claim | Replacement worker safely processes queued work | Killed child JVM; replacement applied once | PASS, actual process kill against real DB | `killedWorkerBeforeClaimAfterClaimAndAfterCommitRecovers` |
| Process death after claim | No overlapping ownership; claim released on disconnect | Competitor skipped locked transaction; replacement applied after child death | PASS, actual process kill | Same test; JDBC interception pauses only the test child |
| Process death during SQL | No partial commit; safe replay | Killed child while AFTER INSERT trigger was blocked; no receipt/business row survived; replacement succeeded | PASS, actual process kill | `killedWorkerDuringSqlRollsBackAndReplays` |
| DB backend termination | Roll back and recover from connection loss | Terminated backend during blocked write; no partial rows or versions | PASS, DB fault injection | `backendDeathDuringBusinessWriteRollsBackAndNewWorkerRecovers` |
| Process death after commit | Receipt prevents duplicate effects | Killed child after successful commit; replay retained one row, receipt, and version | PASS, DB/process boundary | `killedWorkerBeforeClaimAfterClaimAndAfterCommitRecovers` |
| Lost COMMIT acknowledgement | Do not demote a committed transaction to failure/retry | JDBC wrapper commits then raises SQLSTATE 08006; ledger remains APPLIED | PASS after fix, injected network-outcome ambiguity | `lostCommitAcknowledgementCannotDemoteAppliedReceipt` |
| Commit-before-Kafka-ack crash | Durable staging survives redelivery | Offset helper and destination replay passed; exact live staging-commit/ack kill window was not injected | PARTIAL, not claimed as E2E coverage | Local-consumer helper; real offset replay. This architecture acknowledges staging before later business apply |
| Two workers, same object predecessor | At most one consumes a given version | One writer succeeded; the other failed; one business row/version/receipt remained | PASS, real DB concurrency | `twoWritersCannotConsumeTheSameObjectVersion` |
| Optimistic version conflict | No blind overwrite | Stale expected version rejected; previously committed row value unchanged | PASS, real DB | `appliesWholeTransactionOnceAndRejectsStaleSuccessor` |
| Delete and recreate | Preserve tombstone/version chain | Delete removed row but retained DELETED state; recreation required tombstone; old delete replay was a no-op | PASS, real DB | `deletingThenRecreatingPreservesTombstoneVersionChain` |
| Predicate locks and unknown absence | Validate known state while holding dependency object locks | NOWAIT proved predicate row locked; unknown object no longer satisfies NOT_EXISTS | PASS after fixes | `predicateObjectMustStayLockedUntilCommit`; `unknownObjectIsNotProofOfAbsence` |
| Dependency cycle isolation | Quarantine cycle members, not an unrelated ancestor | Cycle members failed; unrelated ancestor remained READY and applied | PASS after fix | `cycleMustNotQuarantineAnUnrelatedAncestor` |
| Formatting-only duplicate | Semantically identical JSON is a duplicate | Pretty-printed replay did not create quarantine evidence | PASS after fix | `jsonFormattingIsNotAConflictingDuplicate` |
| Conflicting duplicate | Block unapplied conflicting transaction | Conflicting fragment quarantined; transaction became FAILED and could not apply | PASS after fix | `conflictingFragmentMustBlockUnappliedTransaction` |
| Tampered first payload / unknown schema | Reject invalid input before business effects | Payload mismatch and schema 999 could not apply | PASS after fixes | Two passing `ProtocolIntegrityPostgresTest` methods |
| Per-table completion counts | Total count alone is insufficient | Incorrect END collection counts did not produce a manifest | PASS after fix, Flink harness | `endCollectionCountsMustMatchCapturedEvents` |
| Planning frontier | Incomplete earlier transaction blocks later planning | ID `10` no longer overtook earlier ID `9` because of map sorting | PASS after fix, Flink harness | `serializedStateCannotReorderThePlanningFrontier` |
| Source transaction replay | Suppress replay or reproduce identical immutable output | Same source transaction produced changed protocol output | **FAIL** | `transactionReplayMustNotGenerateNewVersionsOrHashes` |
| Manifest descriptor integrity | Hash binds versions and dependencies as well as payload | Changing `newObjectVersion.producerTransactionId` left mutation-set hash unchanged | **FAIL** | `manifestMustBindResultingVersionAndDependencies` |
| Destination Kafka offset replay | Reprocessing retained messages does not reapply business changes | Rewound dedicated group; worker logged `staged=44 applied=0`; counts remained 6 receipts / 18 versions / 0 quarantine | PASS, live Kafka + DB, bounded evidence | Saved consumer-group output, worker log, SQL observations |
| MM2 / independent topic delivery | No correctness dependence on source/destination offsets or cross-topic arrival order | Actual MM2 hop used by live E2E; controlled interleaving/reversal tested separately at component level | PARTIAL combined coverage | Live Kafka offsets/capture inspection plus component tests; no exhaustive MM2 failure campaign |

## 4. Recorded live evidence

The successful probe created fixture ID `371516422` and product `Request3-371516422`. Its recorded output was:

```json
{"visibility_counts":[0,7]}
{"transaction_id":"legacy-postgres/local-1/inventory-v1/761","state":"APPLIED","captured":3,"expected":7,"received":7,"receipt_mutations":7,"versions":7,"dependencies":4}
```

The probe first creates a customer/account predecessor transaction. The measured transaction updates the customer and account and inserts an order. The seven mutations cover customer, address, two phone rows, order, product, and order item. Each observation is one SQL statement, so its counts share one PostgreSQL snapshot. Polling is not by itself exhaustive atomicity proof; real DB rollback/barrier tests provide additional evidence.

An earlier fixture, ID `930001`, produced transaction `legacy-postgres/local-1/inventory-v1/759`. It remained WAITING_DEPENDENCIES because its order mutation used key `order_id` while its order-item dependency expected parent key `id`. Three earlier order snapshot transactions were also waiting. The planner key was corrected for subsequent output. Those four old transactions were deliberately not rewritten or force-applied.

For destination Kafka replay, the first reset attempt was rejected because the stopped consumer group was still Stable. A later check showed Empty / zero members; the reset then succeeded. Following restart:

```text
worker: staged=44 applied=0
applied receipts: 6 before / 6 after
version history: 18 before / 18 after
transaction states: 6 APPLIED, 4 WAITING_DEPENDENCIES before and after
quarantine entries: 0 before / 0 after
```

This was a replay of destination control/mutation topics, **not** a successful raw CDC replay through the source planner.

The first live Flink checkpoint metadata showed `<checkpoint-not-externally-addressable>`. After correcting checkpoint configuration and directory ownership, a restored job reported `restored=1`, completed external checkpoints, and paths such as `file:/opt/flink/checkpoints/ae3d04b0a1645fa23ff5d4cb20656d98/chk-3299`.

**Final-state limitation:** the successful live E2E run preceded the last per-collection-count and payload/schema validation edits. Those edits passed their focused automated regressions. A final projector restore was initiated before interruption, but its completion and the final full-stack rerun were not verified. This documentation-only request does not resume them. Inspect running jobs before manually proceeding; do not assume that the last attempted submission is running.

## 5. Defects fixed during Request 3

| Area | Reproduced defect | Correction |
|---|---|---|
| Docker build | Local worker build failed with `A path must be specified!` | Preserved repository directory layout inside build image so included Gradle builds resolve correctly |
| Topic initialization | Compose split the shell program; topic init exited 2 | Passed multiline program as one command-list argument |
| Flink submission | Incorrect command folding produced `Java program should be specified a JAR file` | Explicit shell argument list and preserved continuations |
| Checkpoint storage | Wrong property left checkpoints non-external; mounted directory was root-owned | `state.checkpoints.dir`, image directory ownership, corrected existing test volume ownership |
| MM2 diagnostics | Logging configuration path did not exist | Configured existing `/etc/kafka/connect-log4j.properties` path; new setting was not separately runtime-certified |
| Order projection | Order key did not match destination PK or parent dependency key | Use destination `id` key |
| Frontier state | Protocol map sorting reordered the assembly queue after serialization | Separate state serializer preserving insertion order |
| Timeout state | Late data or stale INCOMPLETE markers reopened DLQ work | Preserve terminal DLQ state; require explicit requeue |
| Source position | Late row fragment overwrote recorded END position | Preserve END position once known |
| Completeness | END per-collection counts ignored | Record and validate collection coverage as well as total/sequence coverage |
| Inbox replay | Byte-format changes looked like content conflicts | Compare semantic JSON for inbox deduplication, including stored older messages |
| Conflicting fragments | Quarantine did not stop a pending transaction | Mark pending transaction FAILED while preserving APPLIED/DLQ terminal records |
| Ambiguous COMMIT | Failure reporting demoted successfully committed work | Reacquire transaction lock and inspect durable receipt before recording failure |
| Cycle handling | Ancestors outside the cycle were quarantined | Restrict affected nodes to mutually reachable dependency members |
| Waiting work | Producer/stager race could miss readiness wake-up | Add an independent readiness sweep to the drain path |
| Locking | Predicate objects not locked; baseline creation could acquire locks in a different order | Canonical combined write/predicate lock order, including creation; reject multiple net writes to one object at apply |
| Absence predicate | Missing state incorrectly satisfied NOT_EXISTS | Treat unknown state as unsatisfied |
| Input validation | First-arrival payload corruption and unsupported schema could apply | Validate payload checksum and supported schema/projection versions at staging |

No SQL migration was introduced in Request 3. Existing persisted incorrect keys/manifests were not migrated.

## 6. Outstanding findings and limits

### Confirmed release blockers — high severity, high confidence

1. **Source replay is not immutable.** The projector removes completed assembly evidence and replans replayed data using already advanced object state. Control hashing also includes processing-time/transport-derived fields. The failing harness test reproduces changed output. Stable replay evidence, boundary-qualified transaction identity, deterministic sequence assignment, and a compatible migration/rebootstrap path are required. Do not simply reset source offsets and assume safety.
2. **Manifest hashing does not bind the full mutation descriptor.** `ProtocolCodec.mutationSetHash` hashes sequence, mutation ID, and claimed payload hash, omitting explicit versions/dependencies. The regression changes the resulting version producer and receives the same hash. Payload validation added in this phase does not fix descriptor integrity. Canonical descriptor/manifest validation needs an explicit wire-format compatibility decision before existing history is changed.

### Other gaps — not an exhaustive release audit

- Snapshot rows still use the simplified per-record path; the approved baseline activation/contribution-state protocol is not certified. Unknown state handling for write baselines is broader than the NOT_EXISTS fix.
- The existing projection implementation does not establish full cross-transaction contribution ownership or general repeated-write netting. Broader update/delete/key-change cases need additional validation.
- Source timers were tested with controlled Flink processing time, not a 24-hour wall-clock fault test. There is no proven autonomous destination timeout for a fragment lost only after source completion.
- Exact-current read dependency/anti-dependency semantics and unobserved source reads were not certified.
- Canonical JSON payload hashing, raw transaction-ID reuse, first-seen/commit-sequence replay stability, malformed-message ingestion, and schema migration need further contract work.
- No deadlock occurred in the deterministic scenarios executed. This does not prove deadlock freedom under arbitrary overlapping workloads. No long-running starvation or retry-storm stress test was performed.
- The source stores growing correctness data in one serialized state value; inbox, receipts, version history, outbox, and quarantine accumulate. No measured capacity limit, growth bound, or safe replay-floor cleanup was established. Do not add a TTL that discards correctness evidence.
- The four old WAITING transactions are known stuck states, not healthy outstanding work. They need an approved repair/rebootstrap procedure.
- No duplicate destination effects were observed in the executed tests, but the failing source-replay invariant prevents a general replay-safe claim.
- Whole JobManager loss, deployment restart without a supplied savepoint, retention exhaustion, audited DLQ requeue, and outbox publication recovery remain unverified.
- No Oracle, Aurora/MSK/AWS deployment, large-transaction, production-load, or cross-database atomicity validation was performed.

The intended guarantee remains **at-least-once transport + replay-safe idempotent destination effects + atomicity inside one destination database**. This report does not certify it end to end and does not claim distributed exactly-once processing.

## 7. Files changed or added in this phase

This list distinguishes Request 3 work from the large pre-existing Request 2 dirty worktree.

| File / directory | Request 3 change |
|---|---|
| `transaction-apply/src/main/java/com/example/cdc/apply/TransactionApplyService.java` | Staging validation, terminal state protection, failure recovery, readiness sweep, cycle isolation, predicate/write locking |
| `transaction-apply/build.gradle` | Test subprocess classpath |
| `transaction-apply/src/test/java/com/example/cdc/apply/TransactionApplyServicePostgresTest.java` | Shared package-visible fixtures/helpers |
| `transaction-apply/src/test/java/com/example/cdc/apply/AdversarialApplyPostgresTest.java` | 17 adversarial real-DB tests |
| `transaction-apply/src/test/java/com/example/cdc/apply/CrashWorker.java` | Killable test JVM with test-only JDBC pause points |
| `transaction-apply/src/test/java/com/example/cdc/apply/ProtocolIntegrityPostgresTest.java` | Three integrity release-gate tests |
| `transaction-projector/build.gradle` | Flink test-harness dependencies |
| `transaction-projector/Dockerfile` | Writable checkpoint directory for fresh deployments |
| `transaction-projector/src/main/java/com/example/cdc/projector/TransactionProjectorFunction.java` | State ordering, DLQ guard, END preservation, collection counts |
| `transaction-projector/src/main/java/com/example/cdc/projector/ProjectorState.java` | Expected per-collection counts |
| `transaction-projector/src/main/java/com/example/cdc/projector/AssemblyRules.java` | Collection completeness validation |
| `transaction-projector/src/main/java/com/example/cdc/projector/InventoryProjectionPlanner.java` | Correct destination order key |
| `transaction-projector/src/test/java/com/example/cdc/projector/ProjectorFailureTest.java` | Five harness/recovery/replay tests |
| `transaction-projector/src/test/java/com/example/cdc/projector/InventoryProjectionPlannerTest.java` | Order-key regression |
| `transaction-projector/src/test/java/com/example/cdc/projector/AssemblyRulesTest.java` | Collection-count fixtures |
| `local/cloud/lambda-consumer/Dockerfile` | Build layout fix |
| `local/source/docker-compose.yml` | Shell argument fixes, checkpoint property, MM2 logging configuration |
| `local/tests/test-transaction-request3.py` | Non-resetting live E2E probe |
| This document and implementation-record status link | Saved findings, results, and manual instructions |

## 8. How to run manually later

**These commands have not been executed as part of this summary request.** Run them only when you choose to resume testing. Expect the two documented failing tests until their fixes are approved and implemented.

### A. Prerequisites and automated tests

Use a local development machine with Gradle, Python 3, and a Java 17 toolchain available. The recorded environment used macOS, Docker Desktop, host Gradle 9.5.1, and Corretto 21 to launch Gradle with the project's Java 17 toolchain. Docker builds use their pinned Gradle/JDK images. Dependency downloads may need network access.

The PostgreSQL component tests start isolated embedded PostgreSQL instances; they do not require the Docker stack. They truncate their own test tables, create test triggers, start/kill child Java processes, and terminate test DB backends. Do not repoint them at a real/shared database.

```bash
cd /Users/yuriy/work/Terraform/MyProject/AWS/awsKafka2Msk2DB
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home
export GRADLE_USER_HOME=/tmp/cdc-transaction-gradle

gradle -p transaction-protocol test --rerun-tasks --no-daemon
gradle -p transaction-apply test --rerun-tasks --no-daemon
gradle -p transaction-projector test --rerun-tasks --no-daemon
gradle -p local/cloud/lambda-consumer test --rerun-tasks --no-daemon
gradle -p lambda_java test --rerun-tasks --no-daemon
```

Execute these serially: the modules share included builds and generated outputs. If a command fails, inspect its report and continue manually only if appropriate. Apply and projector currently each have one expected **real defect**, not an ignored/flaky test.

To isolate a scenario:

```bash
gradle -p transaction-apply test --tests '*AdversarialApplyPostgresTest.killedWorkerDuringSqlRollsBackAndReplays' --rerun-tasks --no-daemon
gradle -p transaction-apply test --tests '*ProtocolIntegrityPostgresTest' --rerun-tasks --no-daemon
gradle -p transaction-projector test --tests '*ProjectorFailureTest' --rerun-tasks --no-daemon
```

Open the module's `build/reports/tests/test/index.html` in a browser, or inspect its `build/test-results/test/TEST-*.xml`. Running one selected test can replace that module's prior full-suite report.

### B. Inspect the existing local stack first

```bash
docker ps --format '{{.Names}} {{.Status}}'
curl -fsS http://localhost:8081/jobs/overview
curl -fsS http://localhost:8083/connectors/legacy-postgres-source/status
docker logs --tail 80 local-cloud-lambda
docker logs --tail 80 local-source-mirrormaker2
```

Check that exactly the intended projector job is RUNNING and the Debezium task—not only its connector—is RUNNING. Before upgrading an existing projector, take a savepoint and explicitly restore it. Do not start another job at earliest offsets against this generation. Changing a checkpoint property does not automatically restore state.

Preserved savepoints from this investigation included:

```text
file:/opt/flink/checkpoints/savepoint-1e3b1d-a2de9ee32dd6
file:/opt/flink/checkpoints/savepoint-ae3d04-034cf8d099e1
```

These are container-volume paths, not host paths or recommendations to rewind to an older cut. The final attempted restore used the second savepoint and `/tmp/request3-projector-final.jar` inside the JobManager container. Its completion is not certified here. `/tmp` artifacts are not a durable deployment mechanism.

For a **fresh disposable local environment only**, the startup sequence is:

```bash
docker network inspect cdc-migration-local || docker network create cdc-migration-local
docker compose -f local/cloud/docker-compose.yml up -d --build cloud-destination-postgres cloud-kafka local-lambda
docker compose -f local/source/docker-compose.yml build source-flink-jobmanager
docker compose -f local/source/docker-compose.yml up -d source-mirrormaker2
```

This creates/starts development resources and inserts the repository's source seed data. It is not a production deployment recipe. Check for existing containers/ports first (notably 5432, 5434, 9092, 19092, 8081, 8083). On first startup, the consumer can initially see only partition 0 before MM2 expands topics; allow metadata refresh/rebalance and inspect assignments before testing. Do not use `RESET=1`, `down -v`, or delete existing volumes to obtain a green result.

### C. Run the live atomicity probe

With the intended stack healthy:

```bash
python3 local/tests/test-transaction-request3.py
```

The probe creates a new test fixture, waits for its predecessor, writes the three-table transaction, polls one-snapshot destination counts, and checks the ledger plus seven version records. It prints the fixture ID and transaction ID. The per-phase wait timeout is 120 seconds; Docker/SQL subprocesses have 30-second timeouts. It leaves test rows and protocol evidence in place. A timeout is a failure to investigate, not permission to force READY or delete a dependency.

Expected shape of a successful result:

```text
{"fixture_id": ..., "product": "Request3-..."}
{"visibility_counts": [0, 7]}
{"transaction_id": "...", "state": "APPLIED", "captured": 3, "expected": 7, "received": 7, "receipt_mutations": 7, "versions": 7, "dependencies": ...}
PASS: source PostgreSQL -> Debezium -> Flink -> Kafka -> MM2 -> destination atomic commit
```

A fast run may only sample `[7]`; the script accepts either 0 or 7 per observation. Any intermediate count fails. This is one E2E scenario, not the entire fault matrix.

### D. Inspect DB and Kafka evidence

```bash
docker exec local-cloud-destination-postgres psql -U appuser -d destinationdb -c "SELECT transaction_id,state,expected_mutation_count,received_mutation_count,last_error FROM cdc.tx_transaction ORDER BY source_commit_sequence;"
docker exec local-cloud-destination-postgres psql -U appuser -d destinationdb -c "SELECT transaction_id,mutation_count,committed_at FROM cdc.applied_transaction ORDER BY committed_at DESC;"
docker exec local-cloud-destination-postgres psql -U appuser -d destinationdb -c "SELECT transaction_id,dependency_type,producer_transaction_id,object_key FROM cdc.dependency ORDER BY transaction_id;"
docker exec local-cloud-destination-postgres psql -U appuser -d destinationdb -c "SELECT count(*) AS versions FROM cdc.version_history; SELECT count(*) AS quarantined FROM cdc.quarantine;"
docker exec local-source-kafka kafka-get-offsets --bootstrap-server source-kafka:29092 --topic 'cdc.tx.*'
docker exec local-cloud-kafka kafka-get-offsets --bootstrap-server cloud-kafka:39092 --topic 'cdc.tx.*'
docker exec local-cloud-kafka kafka-consumer-groups --bootstrap-server cloud-kafka:39092 --group local-lambda-to-postgres --describe
```

For a small committed control-record sample:

```bash
docker exec local-cloud-kafka kafka-console-consumer --bootstrap-server cloud-kafka:39092 --topic cdc.tx.control.v1 --from-beginning --isolation-level read_committed --max-messages 1 --timeout-ms 10000
```

Offsets are transport diagnostics, not object versions. Equal source/target offsets are not a general correctness condition.

### E. Optional destination-only replay

This changes the dedicated **local test consumer group's offsets**. Record DB counts/rows first. Do not perform it on a production group or on the source projector group.

```bash
docker stop local-cloud-lambda
docker exec local-cloud-kafka kafka-consumer-groups --bootstrap-server cloud-kafka:39092 --group local-lambda-to-postgres --describe --state
```

Wait until the reported state is Empty with zero members; stopping the process does not immediately expire group membership. Then:

```bash
docker exec local-cloud-kafka kafka-consumer-groups --bootstrap-server cloud-kafka:39092 --group local-lambda-to-postgres --reset-offsets --all-topics --to-earliest --execute
docker compose -f local/cloud/docker-compose.yml start local-lambda
docker logs --tail 80 local-cloud-lambda
```

Check the reset output itself: the CLI previously printed a reset rejection despite returning exit status 0. Verify group lag drains and compare business rows, receipts, and version history with the pre-replay evidence. Do not confuse destination replay success with the currently failing source-replay invariant.

## DECISIONS REQUIRING APPROVAL

Before further semantic/compatibility changes:

1. Approve how to introduce complete canonical descriptor/manifest hashing without silently changing existing v1 identities and hashes: a versioned wire-format transition/new projection generation is recommended.
2. Approve the migration/rebootstrap treatment of already persisted source identities, planned versions, and the four stuck local transactions. Retain old evidence; do not rewrite applied receipts. Source replay repair must remain consistent with the approved stable-version semantics.
3. Confirm the baseline/cutover and incomplete-transport recovery work needed to finish the original design; passing this limited probe must not be accepted as baseline certification.

No approval to weaken an invariant is requested or assumed. No new wire protocol, generation migration, automatic requeue, or history deletion was implemented. Further testing remains stopped at the user's request.
