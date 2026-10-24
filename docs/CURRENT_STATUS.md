# Current status

As of **2026-10-23**, the simplified local transaction-boundary lab is green.

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
| Shell syntax checks | passed |
| Clean-volume full local Debezium/Flink/MM2/apply E2E | passed |
| First cross-schema receipt | 4 mutations |
| Second cross-schema update receipt | 2 mutations |

## AWS boundary

No AWS API, deployment, or real AWS test was used. Terraform, `user_data.sh.tpl`, Kafka UI, the private network-test EC2 host, and AWS scripts were not modified. Shared Java/SQL artifacts now implement the local simplification while AWS wiring still describes the earlier baseline/six-table contract. Reconcile those differences and perform an isolated AWS rehearsal before any AWS deployment.
