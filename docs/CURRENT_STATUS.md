# Current status

As of **2026-10-12**, the active implementation is the normalized, source-owned row-version protocol.

## Verdict

**Focused verification and the clean local full-stack path are green. AWS deployment verification is not yet recorded.**

No AWS deployment, Terraform apply/destroy, shared database reset, commit, or push was performed as part of this change.

## Active model

- Six source tables map one-to-one to six destination tables by primary key: customers, addresses, contact numbers, products, orders, and order items.
- Every source business row has a database-managed numeric version and a retained owner-state/journal lineage.
- A source transaction has one stable UUID and an ordered append-only revision journal.
- The projector emits one direct mutation for each changed source owner and derives only foreign-key existence dependencies.
- The destination checks numeric versions on business rows and commits the full mutation set, receipt, owner state, and outbox atomically.
- The only protocol integrity digest retained is `mutationSetHash`, computed over the complete canonical mutation descriptors.
- The active runtime has no compatibility selector or alternate protocol path.

The source has no projection tables, multi-writer contribution arbitration, dirty-owner queue, deferred projection finalizer, catalog-delete API, parent fence owners, or process-wide writer lock.

## Fresh evidence

| Check | Result |
|---|---:|
| `transaction-protocol` Gradle tests | passed |
| `transaction-projector` Gradle tests | passed |
| `transaction-apply` Gradle tests, including embedded PostgreSQL | passed |
| Disposable PostgreSQL source trigger/journal integration suite | 6/6 passed |
| Real Debezium capture fixture and projector replay test | passed |
| Clean-volume two-Compose baseline + seven-mutation atomic E2E | passed |
| Compose configuration validation | passed |
| Terraform format and validation | passed |

The source integration suite verifies the normalized table inventory, publication contents, absence of retired source schemas/tables, write admission, rollback, numeric lifecycle, delete/re-create, primary-key-change rejection, logical decoding, overflow, concurrent independent writers, and restart persistence.

## Remaining release work

- Exercise assembled-container restart/replay fault injection beyond the clean startup path.
- Run an isolated AWS rehearsal before declaring the deployment release-ready.

Superseded design, validation, command-history, and generated prompt documents were removed. This status, the operations guide, and the executable schema/tests define the active posture.
