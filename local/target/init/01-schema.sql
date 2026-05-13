CREATE TABLE IF NOT EXISTS customer_projection (
  id BIGINT PRIMARY KEY,
  email TEXT,
  full_name TEXT NOT NULL,
  status TEXT NOT NULL,
  source_system TEXT NOT NULL DEFAULT 'legacy-postgres',
  source_table TEXT NOT NULL DEFAULT 'inventory.customers',
  source_lsn TEXT,
  source_tx_id TEXT,
  source_event_time TEXT,
  ingested_at TEXT NOT NULL DEFAULT NOW()::TEXT,
  schema_version TEXT NOT NULL DEFAULT 'v1'
);

CREATE SCHEMA IF NOT EXISTS operational;
CREATE SCHEMA IF NOT EXISTS client;

CREATE TABLE IF NOT EXISTS client.customers (
  id BIGINT PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT,
  status TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS client.addresses (
  customer_id BIGINT NOT NULL,
  address_type TEXT NOT NULL,
  street TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT,
  PRIMARY KEY (customer_id, address_type)
);

CREATE TABLE IF NOT EXISTS operational.contact_numbers (
  customer_id BIGINT NOT NULL,
  phone_type TEXT NOT NULL,
  phone_number TEXT NOT NULL,
  PRIMARY KEY (customer_id, phone_type)
);

CREATE TABLE IF NOT EXISTS operational.products (
  name TEXT PRIMARY KEY,
  category TEXT,
  current_price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS operational.orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  order_date TIMESTAMPTZ,
  status TEXT
);

CREATE TABLE IF NOT EXISTS operational.order_items (
  order_id BIGINT NOT NULL,
  product_name TEXT NOT NULL,
  quantity INTEGER,
  price_at_purchase DECIMAL(10,2),
  PRIMARY KEY (order_id, product_name)
);
