CREATE SCHEMA IF NOT EXISTS operational;
CREATE SCHEMA IF NOT EXISTS cdc;

CREATE TABLE IF NOT EXISTS operational.products (
  name TEXT PRIMARY KEY,
  category TEXT,
  current_price DECIMAL(10,2),
  source_record_type TEXT,
  source_ts_ms BIGINT,
  source_tx_id TEXT,
  idempotency_key VARCHAR(255),
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT
);

CREATE TABLE IF NOT EXISTS operational.orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  order_date TIMESTAMPTZ,
  status TEXT,
  source_record_type TEXT,
  source_ts_ms BIGINT,
  source_tx_id TEXT,
  idempotency_key VARCHAR(255),
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT
);

CREATE TABLE IF NOT EXISTS operational.order_items (
  order_id BIGINT NOT NULL,
  product_name TEXT NOT NULL,
  quantity INTEGER,
  price_at_purchase DECIMAL(10,2),
  source_record_type TEXT,
  source_ts_ms BIGINT,
  source_tx_id TEXT,
  idempotency_key VARCHAR(255),
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (order_id, product_name)
);

CREATE TABLE IF NOT EXISTS operational.contact_numbers (
  customer_id BIGINT NOT NULL,
  phone_type TEXT NOT NULL,
  phone_number TEXT NOT NULL,
  source_record_type TEXT,
  source_ts_ms BIGINT,
  source_tx_id TEXT,
  idempotency_key VARCHAR(255),
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (customer_id, phone_type)
);

CREATE TABLE IF NOT EXISTS operational.events (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  kafka_partition INTEGER NOT NULL,
  kafka_offset BIGINT NOT NULL,
  event_ts TIMESTAMPTZ DEFAULT NOW(),
  payload JSONB NOT NULL,
  UNIQUE(topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS cdc.transaction_metadata (
  tx_id TEXT NOT NULL,
  status TEXT NOT NULL,
  event_count BIGINT,
  data_collections JSONB,
  ts_ms BIGINT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (tx_id, status)
);
