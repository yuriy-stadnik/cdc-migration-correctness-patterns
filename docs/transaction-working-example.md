# Working example: one source commit split across two destination schemas

This example is implemented by [`local/scripts/run-local-e2e.sh`](../local/scripts/run-local-e2e.sh). It starts with clean source and destination volumes because the simplified lab uses the fixed `empty-bootstrap-v1` proof for first creation.

Run it from the repository root:

```bash
RESET=1 ./local/scripts/run-local-e2e.sh
```

## Source transaction

One PostgreSQL transaction creates a customer, product, order, and order item:

```sql
BEGIN;
SELECT source_cdc.begin_write();

INSERT INTO inventory.customers(id,first_name,last_name,email,status)
VALUES (12001,'Local','Customer','local-12001@example.com','ACTIVE');

INSERT INTO inventory.products(name,category,current_price)
VALUES ('Local Migration Test Item 10001','Integration',12.50);

INSERT INTO inventory.orders(id,customer_id,status)
VALUES (10001,12001,'COMPLETED');

INSERT INTO inventory.order_items(order_id,product_name,quantity,price_at_purchase)
VALUES (10001,'Local Migration Test Item 10001',4,12.50);

COMMIT;
```

`source_cdc.begin_write()` creates one UUID for the current PostgreSQL transaction. The row triggers use PostgreSQL's internal `pg_current_xact_id()` to find that context, increment each owner's numeric version, update `source_cdc.owner_state`, and append four ordered rows to `source_cdc.revision_journal`.

Each first revision has this predecessor:

```json
{"state":{"presence":"ABSENT","version":null},"producer":{"type":"BASELINE","id":"empty-bootstrap-v1"}}
```

The fixed baseline producer is only an explicit proof that clean volumes contain no earlier owner. It is not a dynamic snapshot.

## Capture and projection

Debezium publishes the four business changes, four journal rows, transaction context, and BEGIN/END metadata to `cdc.source.capture.v2`. [`V2Capture.java`](../transaction-projector/src/main/java/com/example/cdc/projector/V2Capture.java) joins those records using the UUID, native XID/LSN interval, event counts, and journal ordinals.

[`V2Projector.java`](../transaction-projector/src/main/java/com/example/cdc/projector/V2Projector.java) reduces the evidence to four owner transitions and four destination mutations:

| Source owner | Destination object |
|---|---|
| `inventory.customers/id=12001` | `client.customers/id=12001` |
| `inventory.products/name=...` | `operational.products/name=...` |
| `inventory.orders/id=10001` | `operational.orders/id=10001` |
| `inventory.order_items/(10001,...)` | `operational.order_items/(10001,...)` |

The order requires the customer, while the order item requires the order and product. Because all parents are created in the same source transaction, their final states are validated as one unit. The projector emits mutations to `cdc.tx.mutations.v2` and one manifest to `cdc.tx.control.v2`; the manifest's `mutationSetHash` binds all four canonical mutation descriptors.

## Local transport and atomic apply

[`local/source/mm2.properties`](../local/source/mm2.properties) replicates only control, mutation, and DLQ topics to the cloud-side Kafka emulator. The local apply worker stages records from both protocol topics.

When the manifest and all four mutations are present, [`V2TransactionApplyService.java`](../transaction-apply/src/main/java/com/example/cdc/apply/V2TransactionApplyService.java) verifies:

- the manifest hash and sequence set;
- the empty-bootstrap predecessors;
- the dependency graph and absence of cycles;
- before-images, versions, and parent relationships.

It then inserts `client.customers`, all three `operational.*` rows, four producer rows, four owner-state rows, and one `cdc.v2_receipt` in a single destination PostgreSQL transaction. The split is logical—two schemas—not two databases—so rollback remains atomic across both groups.

## Second transaction: transaction lineage

The E2E script then commits two updates together:

```sql
BEGIN;
SELECT source_cdc.begin_write();
UPDATE inventory.customers SET status='VERIFIED',updated_at=now() WHERE id=12001;
UPDATE inventory.orders SET status='SHIPPED' WHERE id=10001;
COMMIT;
```

Both owners move from version 1 to version 2. Their predecessors use `ProducerType.TRANSACTION` and name the exact first transaction ID. The destination produces one new receipt with `mutation_count=2`; both rows name that same second transaction in `source_tx_id`.

## Verification queries

```sql
SELECT transaction_id,mutation_set_hash,mutation_count,committed_at
FROM cdc.v2_receipt ORDER BY committed_at;

SELECT owner->>'collection' AS collection,version,presence,producer_type,producer_id
FROM cdc.v2_owner_state ORDER BY collection,owner_key;

SELECT * FROM cdc.transaction_dependency_graph
ORDER BY dependent_transaction_id,owner_key;
```

Expected receipt mutation counts are `4` for the insert transaction and `2` for the update transaction.

## AWS note

The AWS E2E script executes the same SQL files used by the local script. Terraform derives Lambda's MSK topics from the shared topic specification, and EC2 user data installs the shared source SQL, connector configuration, MM2 defaults, and E2E SQL. The AWS adapter is prepared for this example but remains unverified against real AWS services until an isolated AWS rehearsal is run.
