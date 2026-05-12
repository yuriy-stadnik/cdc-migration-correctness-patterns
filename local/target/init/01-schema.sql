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
