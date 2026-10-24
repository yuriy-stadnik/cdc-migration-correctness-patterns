# Current status

As of **2026-10-24**, the simplified local transaction-boundary lab is green and the AWS deployment wiring is aligned but not deployed or AWS-tested.

## Verdict

The local path preserves a source PostgreSQL transaction while splitting its rows between `client` and `operational` schemas in one destination database transaction. AWS deployment readiness is intentionally not claimed.

## Active local model

- Four source tables map one-to-one by primary key: customers, products, orders, and order items.
- The destination split is `client.customers` plus three `operational.*` tables.
- Source-owned numeric versions, `source_cdc.owner_state`, and append-only `source_cdc.revision_journal` remain.
- `ProducerType`, exact transaction producer lineage, the dependency graph, and cycle detection remain.
- Separate `cdc.tx.control.v2`, `cdc.tx.mutations.v2`, and `cdc.tx.dlq.v2` topics remain.
- Clean local volumes use the fixed `BASELINE/empty-bootstrap-v1` proof only for first-creation `ABSENT` state.
- The destination atomically commits business rows, owner state, producer evidence, and one immutable receipt.

Removed local complexity includes dynamic baseline reconciliation/topics/tables, addresses/contact numbers, global advisory and logical owner locks, destination outbox, and persistent quarantine storage. A single local apply worker is assumed.

## Fresh evidence

| Check | Result |
|---|---:|
| `transaction-protocol` Gradle tests | passed |
| `transaction-projector` Gradle tests, including cycle detection | passed |
| `transaction-apply` Gradle tests with embedded PostgreSQL | passed |
| Local Kafka apply-consumer Gradle tests | passed |
| Disposable PostgreSQL source trigger/journal suite | 6/6 passed |
| Source and cloud Compose configuration validation | passed |
| Rendered EC2 user-data Bash and generated Compose validation | passed |
| Terraform formatting and offline configuration validation | passed |
| Shared projector and Lambda artifact build/tests | passed |
| Shell syntax checks | passed |
| Clean-volume full local Debezium/Flink/MM2/apply E2E | passed |
| First cross-schema receipt | 4 mutations |
| Second cross-schema update receipt | 2 mutations |

## AWS preparation

- Local Compose and AWS EC2 bootstrap share source SQL, connector JSON, topic specifications, MM2 defaults, and E2E transaction SQL.
- The projector and Lambda artifacts build from the same protocol/apply modules through `scripts/build-deployment-artifacts.sh`.
- Terraform derives Lambda's three MSK topics from the shared topic file.
- Lambda reserved concurrency is one, matching the single-writer apply model.
- Source schema version 4 and the destination migration checksum reject incompatible retained lab volumes.

No AWS API, deployment, or real AWS test was used. AWS remains unverified until an isolated rehearsal is performed.
