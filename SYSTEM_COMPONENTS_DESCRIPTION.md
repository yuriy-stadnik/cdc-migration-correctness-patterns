# System components

## Source PostgreSQL

The example source is normalized into six directly owned tables:

- `inventory.customers(id)`
- `inventory.addresses(customer_id,address_type)`
- `inventory.contact_numbers(customer_id,phone_type)`
- `inventory.products(name)`
- `inventory.orders(id)`
- `inventory.order_items(order_id,product_name)`

Each row maps to exactly one destination row with the same key. Database triggers manage `version BIGINT`, require an explicit `source_cdc.begin_write()` transaction context, retain deletion lineage in `source_cdc.owner_state`, and append source revisions to `source_cdc.revision_journal`.

## Debezium and Kafka

Debezium captures the six business tables plus `source_cdc.transaction_context` and `source_cdc.revision_journal`. The connector routes them and PostgreSQL transaction metadata to the single-partition `cdc.source.capture.v2` topic.

## Transaction projector

The Flink job verifies complete source transactions, journal/row agreement, row-version continuity, and source foreign-key dependencies. It emits immutable manifests and numbered mutations to:

- `cdc.tx.control.v2`
- `cdc.tx.mutations.v2`
- `cdc.tx.dlq.v2`

Controlled reconciliation uses `cdc.source.baseline.control.v2` and `cdc.tx.baseline.v2`.

## Replication and apply

MirrorMaker 2 copies the transaction topics to MSK or the local cloud Kafka emulator. Lambda in AWS, or the local Java consumer, durably stages messages and applies complete transactions to one PostgreSQL database containing `client`, `operational`, and `cdc` schemas.

Business rows carry their source-owned numeric `version`, `source_tx_id`, and an idempotency key. The apply transaction verifies the complete `mutationSetHash`, expected versions, dependencies, and mutation sequence before committing business changes and a receipt atomically.

## Direct table mapping

| Source | Destination |
|---|---|
| `inventory.customers` | `client.customers` |
| `inventory.addresses` | `client.addresses` |
| `inventory.contact_numbers` | `operational.contact_numbers` |
| `inventory.products` | `operational.products` |
| `inventory.orders` | `operational.orders` |
| `inventory.order_items` | `operational.order_items` |
