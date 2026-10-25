-- The example source is intentionally normalized so every captured row owns
-- exactly one destination row with the same primary key. No destination row
-- is assembled from competing source rows.
CREATE TABLE IF NOT EXISTS inventory.customers (
  id BIGINT PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory.products (
  name TEXT PRIMARY KEY,
  category TEXT,
  current_price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS inventory.orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  order_date TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL DEFAULT 'COMPLETED',
  CONSTRAINT fk_inventory_order_customer FOREIGN KEY (customer_id)
    REFERENCES inventory.customers(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS inventory.order_items (
  order_id BIGINT NOT NULL,
  product_name TEXT NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  price_at_purchase DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (order_id, product_name),
  CONSTRAINT fk_inventory_item_order FOREIGN KEY (order_id)
    REFERENCES inventory.orders(id) DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT fk_inventory_item_product FOREIGN KEY (product_name)
    REFERENCES inventory.products(name) DEFERRABLE INITIALLY DEFERRED
);

ALTER TABLE inventory.customers REPLICA IDENTITY FULL;
ALTER TABLE inventory.products REPLICA IDENTITY FULL;
ALTER TABLE inventory.orders REPLICA IDENTITY FULL;
ALTER TABLE inventory.order_items REPLICA IDENTITY FULL;

-- The v2 lab starts empty. The first transaction is therefore rooted in the
-- fixed empty-bootstrap lineage and needs no snapshot/baseline reconciliation.
