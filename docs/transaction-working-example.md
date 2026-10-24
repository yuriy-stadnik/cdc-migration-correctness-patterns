# Working transaction example

This walkthrough follows one committed PostgreSQL transaction through source versioning, Debezium capture, Flink aggregation, Kafka replication, and atomic destination application. It uses the same transaction as the local end-to-end test in [`local/scripts/run-local-e2e.sh`](../local/scripts/run-local-e2e.sh).

The example requires an active reconciled baseline. The local end-to-end script creates and publishes one automatically when the destination namespace is empty. AWS creates the baseline topics but currently requires a separate reconciliation and publication step before live transactions can be released by the projector.

## Run the complete local example

From the repository root:

```bash
RESET=1 ./local/scripts/run-local-e2e.sh
```

The script starts the source and cloud-emulator Compose projects, activates a baseline, commits the example source transaction, and waits for one destination receipt containing seven mutations.

## 1. Commit one source transaction

The executable transaction is defined at [`local/scripts/run-local-e2e.sh`](../local/scripts/run-local-e2e.sh#L94). With illustrative values `CUSTOMER_ID=12001` and `ORDER_ID=10001`, its business operations are equivalent to:

```sql
BEGIN;
SELECT source_cdc.begin_write();

INSERT INTO inventory.customers
  (id,first_name,last_name,email,status,updated_at)
VALUES
  (12001,'Local','Customer','local-12001@example.com','ACTIVE',now());

INSERT INTO inventory.addresses
  (customer_id,address_type,street,city,state,zip_code)
VALUES
  (12001,'HOME','789 Pine Rd','Ridgewood','NJ','07450');

INSERT INTO inventory.contact_numbers
  (customer_id,phone_type,phone_number)
VALUES
  (12001,'HOME','201-555-7777'),
  (12001,'MOBILE','201-555-7778');

INSERT INTO inventory.products
  (name,category,current_price)
VALUES
  ('Local Migration Test Item 10001','Integration',12.50);

INSERT INTO inventory.orders
  (id,customer_id,status)
VALUES
  (10001,12001,'COMPLETED');

INSERT INTO inventory.order_items
  (order_id,product_name,quantity,price_at_purchase)
VALUES
  (10001,'Local Migration Test Item 10001',4,12.50);

COMMIT;
```

`source_cdc.begin_write()` assigns one stable source UUID to the PostgreSQL transaction. Source DML without that context is rejected. The source functions and triggers are defined in [`local/postgres/init/03-source-version-v2.sql`](../local/postgres/init/03-source-version-v2.sql#L35).

The transaction changes seven source owners:

| Source collection | Owner key | Initial state | Final state |
|---|---|---|---|
| `inventory.customers` | `id=12001` | `ABSENT` | `PRESENT`, version 1 |
| `inventory.addresses` | `customer_id=12001,address_type=HOME` | `ABSENT` | `PRESENT`, version 1 |
| `inventory.contact_numbers` | `customer_id=12001,phone_type=HOME` | `ABSENT` | `PRESENT`, version 1 |
| `inventory.contact_numbers` | `customer_id=12001,phone_type=MOBILE` | `ABSENT` | `PRESENT`, version 1 |
| `inventory.products` | `name=Local Migration Test Item 10001` | `ABSENT` | `PRESENT`, version 1 |
| `inventory.orders` | `id=10001` | `ABSENT` | `PRESENT`, version 1 |
| `inventory.order_items` | `order_id=10001,product_name=Local Migration Test Item 10001` | `ABSENT` | `PRESENT`, version 1 |

For every changed owner, the triggers:

1. assign or advance the source-owned numeric `version`;
2. update `source_cdc.owner_state`;
3. append an ordered entry to `source_cdc.revision_journal`; and
4. increment the transaction's `journal_count`.

All business rows and source CDC evidence commit in the same PostgreSQL transaction.

## 2. Capture the committed transaction

Debezium captures the six business tables, `source_cdc.transaction_context`, `source_cdc.revision_journal`, and transaction boundary metadata. Its routing configuration sends all these records to the single-partition `cdc.source.capture.v2` topic. See [`local/source/connectors/source-version-v2-config.json`](../local/source/connectors/source-version-v2-config.json#L11).

The topic therefore contains more than seven Kafka records: it contains business changes plus the source context, journal, and BEGIN/END evidence needed to prove that the source transaction is complete. Kafka offsets are transport positions; they are not used as source row versions or permanent transaction identity.

## 3. Aggregate source evidence in the projector

The Flink entry point in [`TransactionProjectorJob.java`](../transaction-projector/src/main/java/com/example/cdc/projector/TransactionProjectorJob.java#L18) reads the capture and baseline topics with `read_committed` isolation and sends one database/incarnation/scope lane through the stateful projector.

[`V2Capture.java`](../transaction-projector/src/main/java/com/example/cdc/projector/V2Capture.java#L77) waits for the complete source transaction and verifies:

- matching BEGIN and END evidence;
- continuous Debezium `total_order` values;
- source XID and PostgreSQL LSN agreement;
- declared and observed collection counts;
- the stable UUID from `source_cdc.transaction_context`;
- `journal_count` and continuous journal ordinals; and
- agreement between business-row images and journal before/after images.

Journal entries are grouped by source owner. Multiple revisions of the same owner inside one PostgreSQL transaction become one net transition while retaining the complete revision chain. In this example, every owner changes once, so the projector produces seven net transitions.

The permanent transaction ID is derived from the source UUID:

```text
source-v2:[sourceDatabaseId,sourceIncarnation,sourceTransactionUuid]
```

The local lane uses source incarnation `local-1`; the EC2/AWS lane uses `aws-1`. The same source UUID in different incarnations therefore cannot collide.

## 4. Produce mutations and a manifest

[`V2Projector.java`](../transaction-projector/src/main/java/com/example/cdc/projector/V2Projector.java#L285) maps the seven final owner transitions directly to destination rows:

| Source | Destination | Mutations in this example |
|---|---|---:|
| `inventory.customers` | `client.customers` | 1 |
| `inventory.addresses` | `client.addresses` | 1 |
| `inventory.contact_numbers` | `operational.contact_numbers` | 2 |
| `inventory.products` | `operational.products` | 1 |
| `inventory.orders` | `operational.orders` | 1 |
| `inventory.order_items` | `operational.order_items` | 1 |

A simplified customer mutation is:

```json
{
  "recordType": "MUTATION",
  "transactionId": "source-v2:[\"legacy-postgres\",\"local-1\",\"<source-uuid>\"]",
  "mutationSequence": 4,
  "owner": {
    "collection": "inventory.customers",
    "primaryKey": {"id": {"type": "int64", "value": "12001"}}
  },
  "object": {
    "entity": "client.customers",
    "primaryKey": {"id": {"type": "int64", "value": "12001"}}
  },
  "operation": "INSERT",
  "expected": {
    "state": {"presence": "ABSENT", "version": null},
    "producer": {"type": "BASELINE", "id": "<baseline-id>"}
  },
  "result": {"presence": "PRESENT", "version": 1},
  "after": {
    "id": 12001,
    "first_name": "Local",
    "last_name": "Customer",
    "email": "local-12001@example.com",
    "status": "ACTIVE",
    "version": 1
  }
}
```

This is an explanatory subset, not a complete wire record. Sequence `4` is the customer's position in this example's canonical owner ordering; it is not the SQL statement order. The complete record also contains protocol versions, capture scope, projection generation, dependencies, mutation identity, idempotency key, and other validated fields defined by [`ProtocolV2.java`](../transaction-protocol/src/main/java/com/example/cdc/protocol/ProtocolV2.java#L187).

The projector publishes:

```text
cdc.tx.mutations.v2
  seven immutable mutation records keyed by transactionId

cdc.tx.control.v2
  one immutable manifest keyed by transactionId
  destinationDescriptorCount = 7
  sourceTransitionCount = 7
  mutationSetHash = SHA-256 over all seven canonical mutation descriptors
```

An invalid or contradictory capture is emitted to `cdc.tx.dlq.v2` instead of being converted into a valid transaction plan.

## 5. Transport through local and AWS pipelines

The protocol records are the same in both environments. Only the transport and runtime adapters change.

```text
Local:
  source PostgreSQL
    -> Debezium
    -> source Kafka
    -> Flink transaction-projector
    -> MirrorMaker 2
    -> cloud Kafka emulator
    -> LocalLambdaConsumer
    -> transaction-apply
    -> destination PostgreSQL

AWS:
  PostgreSQL on the EC2 source emulator
    -> Debezium
    -> Kafka on EC2
    -> Flink transaction-projector on EC2
    -> MirrorMaker 2 with MSK IAM authentication
    -> MSK Serverless
    -> Java Lambda
    -> transaction-apply
    -> Aurora PostgreSQL
```

The local MirrorMaker configuration is [`local/source/mm2.properties`](../local/source/mm2.properties#L6). The AWS configuration is generated in [`user_data.sh.tpl`](../user_data.sh.tpl#L259). Both use identity topic replication and copy only `cdc.tx.control.v2`, `cdc.tx.mutations.v2`, and `cdc.tx.dlq.v2`. The source capture and baseline topics remain source-local.

## 6. Stage and atomically apply the transaction

The local adapter is [`LocalLambdaConsumer.java`](../local/cloud/lambda-consumer/src/main/java/com/example/local/LocalLambdaConsumer.java#L18). The AWS adapter is [`Handler.java`](../lambda_java/src/main/java/com/example/Handler.java#L19). Both call the same `TransactionApplyService` API.

Manifest and mutation records may arrive in any order. [`V2TransactionApplyService.java`](../transaction-apply/src/main/java/com/example/cdc/apply/V2TransactionApplyService.java#L20) stages them in:

- `cdc.v2_transaction`;
- `cdc.v2_mutation`; and
- `cdc.v2_dependency`.

The transaction becomes `READY` only when all seven mutations are present and reproduce the manifest's `mutationSetHash`. The apply step then verifies the active baseline, exact predecessor versions, producer evidence, row payloads, and parent relationships.

[`V2BusinessRows.java`](../transaction-apply/src/main/java/com/example/cdc/apply/V2BusinessRows.java#L14) performs the whitelisted inserts into the `client` and `operational` schemas. In one destination database transaction, the service writes:

- seven business rows;
- current owner state and immutable producer evidence;
- one receipt with `mutation_count=7`; and
- one outbox record.

The atomic commit is implemented in [`V2TransactionApplyService.java`](../transaction-apply/src/main/java/com/example/cdc/apply/V2TransactionApplyService.java#L89). If any validation or database operation fails, none of the seven business changes commit.

## 7. Verify the result

The local script performs these assertions automatically. They can also be inspected manually:

```sql
SELECT transaction_id, mutation_set_hash, mutation_count, committed_at
FROM cdc.v2_receipt
ORDER BY committed_at DESC
LIMIT 5;

SELECT id, version, source_tx_id, idempotency_key
FROM client.customers
ORDER BY id DESC
LIMIT 5;

SELECT id, customer_id, version, source_tx_id
FROM operational.orders
ORDER BY id DESC
LIMIT 5;

SELECT state, count(*)
FROM cdc.v2_transaction
GROUP BY state
ORDER BY state;
```

For a successful run, the newest receipt has `mutation_count=7`, all seven destination rows carry the same `source_tx_id`, and the transaction state is `APPLIED`. Re-delivery of identical Kafka records is a replay-safe no-op because the mutation descriptors and receipt are immutable. A conflicting replay is quarantined.
