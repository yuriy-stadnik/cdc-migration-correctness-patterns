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

CREATE TABLE IF NOT EXISTS inventory.addresses (
  customer_id BIGINT NOT NULL,
  address_type TEXT NOT NULL CHECK (address_type IN ('HOME','WORK','OTHER')),
  street TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT,
  PRIMARY KEY (customer_id, address_type),
  CONSTRAINT fk_inventory_address_customer FOREIGN KEY (customer_id)
    REFERENCES inventory.customers(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS inventory.contact_numbers (
  customer_id BIGINT NOT NULL,
  phone_type TEXT NOT NULL CHECK (phone_type IN ('HOME','WORK','MOBILE')),
  phone_number TEXT NOT NULL,
  PRIMARY KEY (customer_id, phone_type),
  CONSTRAINT fk_inventory_contact_customer FOREIGN KEY (customer_id)
    REFERENCES inventory.customers(id) DEFERRABLE INITIALLY DEFERRED
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
ALTER TABLE inventory.addresses REPLICA IDENTITY FULL;
ALTER TABLE inventory.contact_numbers REPLICA IDENTITY FULL;
ALTER TABLE inventory.products REPLICA IDENTITY FULL;
ALTER TABLE inventory.orders REPLICA IDENTITY FULL;
ALTER TABLE inventory.order_items REPLICA IDENTITY FULL;

INSERT INTO inventory.customers (id, first_name, last_name, email, status)
VALUES
  (1, 'Alice', 'Smith', 'alice@example.com', 'ACTIVE'),
  (2, 'Bob', 'Brown', 'bob@example.com', 'ACTIVE')
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory.addresses (customer_id, address_type, street, city, state, zip_code)
VALUES
  (1, 'HOME', '123 Maple St', 'Ridgewood', 'NJ', '07450'),
  (2, 'HOME', '456 Oak Ave', 'Paramus', 'NJ', '07652')
ON CONFLICT (customer_id, address_type) DO NOTHING;

INSERT INTO inventory.contact_numbers (customer_id, phone_type, phone_number)
VALUES
  (1, 'HOME', '201-555-0123'),
  (1, 'MOBILE', '201-555-4567'),
  (2, 'HOME', '201-555-9999')
ON CONFLICT (customer_id, phone_type) DO NOTHING;

INSERT INTO inventory.products (name, category, current_price)
VALUES
  ('Wireless Headphones', 'Electronics', 150.00),
  ('USB-C Cable', 'Electronics', 25.00)
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.orders (id, customer_id, status)
VALUES
  (5001, 1, 'COMPLETED'),
  (5002, 1, 'COMPLETED'),
  (5003, 2, 'COMPLETED')
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory.order_items (order_id, product_name, quantity, price_at_purchase)
VALUES
  (5001, 'Wireless Headphones', 1, 150.00),
  (5002, 'USB-C Cable', 2, 25.00),
  (5003, 'Wireless Headphones', 1, 150.00)
ON CONFLICT (order_id, product_name) DO NOTHING;
