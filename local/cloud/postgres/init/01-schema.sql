CREATE SCHEMA IF NOT EXISTS operational;

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

CREATE TABLE IF NOT EXISTS operational.addresses (
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

CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  kafka_partition INTEGER NOT NULL,
  kafka_offset BIGINT NOT NULL,
  event_ts TIMESTAMPTZ DEFAULT NOW(),
  payload JSONB NOT NULL,
  UNIQUE(topic, kafka_partition, kafka_offset)
);
