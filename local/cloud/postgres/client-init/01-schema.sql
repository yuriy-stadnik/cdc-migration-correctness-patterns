CREATE SCHEMA IF NOT EXISTS client;
CREATE SCHEMA IF NOT EXISTS cdc;

CREATE TABLE IF NOT EXISTS client.customers (
  id BIGINT PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT,
  status TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  source_record_type TEXT,
  source_ts_ms BIGINT,
  source_tx_id TEXT,
  idempotency_key VARCHAR(255),
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT
);

CREATE TABLE IF NOT EXISTS client.addresses (
  customer_id BIGINT NOT NULL,
  address_type TEXT NOT NULL,
  street TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT,
  source_record_type TEXT,
  source_ts_ms BIGINT,
  source_tx_id TEXT,
  idempotency_key VARCHAR(255),
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (customer_id, address_type),
  CONSTRAINT "fk__client.addresses__client.customers"
    FOREIGN KEY (customer_id) REFERENCES client.customers(id)
);

CREATE TABLE IF NOT EXISTS client.events (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  kafka_partition INTEGER NOT NULL,
  kafka_offset BIGINT NOT NULL,
  event_ts TIMESTAMPTZ DEFAULT NOW(),
  payload JSONB NOT NULL,
  UNIQUE(topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS cdc.processed_events (
  event_id TEXT PRIMARY KEY,
  source_tx_id TEXT NOT NULL,
  source_tx_total_order BIGINT,
  target_topic TEXT NOT NULL,
  target_business_key TEXT NOT NULL,
  processed_at TIMESTAMPTZ DEFAULT NOW(),
  payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS cdc.postponed_fk_events (
  event_id TEXT PRIMARY KEY,
  fk_name TEXT NOT NULL,
  child_schema TEXT NOT NULL,
  child_table TEXT NOT NULL,
  parent_schema TEXT NOT NULL,
  parent_table TEXT NOT NULL,
  target_topic TEXT NOT NULL,
  target_business_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  error_message TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'PENDING',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  last_retry_at TIMESTAMPTZ
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
