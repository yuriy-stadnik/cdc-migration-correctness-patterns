BEGIN;
SELECT source_cdc.begin_write();
UPDATE inventory.customers SET status='VERIFIED',updated_at=now() WHERE id=:customer_id;
UPDATE inventory.orders SET status='SHIPPED' WHERE id=:order_id;
COMMIT;
