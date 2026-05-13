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
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (customer_id, address_type)
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

CREATE TABLE IF NOT EXISTS cdc.transaction_metadata (
  tx_id TEXT NOT NULL,
  status TEXT NOT NULL,
  event_count BIGINT,
  data_collections JSONB,
  ts_ms BIGINT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (tx_id, status)
);
