BEGIN;
SELECT source_cdc.begin_write();
INSERT INTO inventory.customers(id,first_name,last_name,email,status,updated_at)
VALUES (:customer_id,'Pipeline','Example',:'email','ACTIVE',now());
INSERT INTO inventory.products(name,category,current_price)
VALUES (:'product','Integration',12.50);
INSERT INTO inventory.orders(id,customer_id,status)
VALUES (:order_id,:customer_id,'COMPLETED');
INSERT INTO inventory.order_items(order_id,product_name,quantity,price_at_purchase)
VALUES (:order_id,:'product',4,12.50);
COMMIT;
