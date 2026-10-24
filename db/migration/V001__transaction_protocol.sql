CREATE SCHEMA IF NOT EXISTS client;
CREATE SCHEMA IF NOT EXISTS operational;
CREATE SCHEMA IF NOT EXISTS cdc;

CREATE TABLE client.customers (
  id BIGINT PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT,
  status TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  version BIGINT NOT NULL CHECK (version > 0),
  source_tx_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE
);
CREATE TABLE operational.products (
  name TEXT PRIMARY KEY,
  category TEXT,
  current_price DECIMAL(10,2),
  version BIGINT NOT NULL CHECK (version > 0),
  source_tx_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE
);
CREATE TABLE operational.orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  order_date TIMESTAMPTZ,
  status TEXT,
  version BIGINT NOT NULL CHECK (version > 0),
  source_tx_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  CONSTRAINT fk_operational_order_customer FOREIGN KEY (customer_id)
    REFERENCES client.customers(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE operational.order_items (
  order_id BIGINT NOT NULL,
  product_name TEXT NOT NULL,
  quantity INTEGER,
  price_at_purchase DECIMAL(10,2),
  version BIGINT NOT NULL CHECK (version > 0),
  source_tx_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  PRIMARY KEY (order_id,product_name),
  CONSTRAINT fk_operational_item_order FOREIGN KEY (order_id)
    REFERENCES operational.orders(id) DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT fk_operational_item_product FOREIGN KEY (product_name)
    REFERENCES operational.products(name) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE cdc.schema_version (
  version INTEGER PRIMARY KEY,
  installed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO cdc.schema_version(version) VALUES (2);

CREATE TABLE cdc.v2_transaction (
  lane TEXT NOT NULL,
  transaction_id TEXT NOT NULL,
  mutation_set_hash TEXT,
  manifest JSONB,
  state TEXT NOT NULL DEFAULT 'COLLECTING' CHECK(state IN ('COLLECTING','READY','WAITING','APPLIED','FAILED')),
  attempts BIGINT NOT NULL DEFAULT 0,
  last_error TEXT,
  PRIMARY KEY(lane,transaction_id)
);
CREATE INDEX v2_transaction_ready ON cdc.v2_transaction(attempts,transaction_id)
  WHERE state IN ('READY','WAITING');
CREATE TABLE cdc.v2_mutation (
  lane TEXT NOT NULL,
  transaction_id TEXT NOT NULL,
  sequence BIGINT NOT NULL CHECK(sequence > 0),
  mutation_id TEXT NOT NULL UNIQUE,
  payload JSONB NOT NULL,
  PRIMARY KEY(lane,transaction_id,sequence),
  FOREIGN KEY(lane,transaction_id) REFERENCES cdc.v2_transaction(lane,transaction_id)
);
CREATE TABLE cdc.v2_dependency (
  lane TEXT NOT NULL,
  transaction_id TEXT NOT NULL,
  dependency_key TEXT NOT NULL,
  dependency_type TEXT NOT NULL,
  owner_key TEXT NOT NULL,
  required_version BIGINT CHECK(required_version > 0),
  required_presence TEXT NOT NULL,
  producer_type TEXT NOT NULL,
  producer_id TEXT NOT NULL,
  PRIMARY KEY(lane,transaction_id,dependency_key),
  FOREIGN KEY(lane,transaction_id) REFERENCES cdc.v2_transaction(lane,transaction_id)
);
CREATE INDEX v2_dependency_producer ON cdc.v2_dependency(lane,producer_id,transaction_id);
CREATE INDEX v2_dependency_owner ON cdc.v2_dependency(lane,owner_key,required_version);
CREATE TABLE cdc.v2_owner_state (
  lane TEXT NOT NULL,
  owner_key TEXT NOT NULL,
  owner JSONB NOT NULL,
  version BIGINT NOT NULL CHECK(version > 0),
  presence TEXT NOT NULL CHECK(presence IN ('PRESENT','DELETED')),
  producer_type TEXT NOT NULL CHECK(producer_type IN ('BASELINE','TRANSACTION')),
  producer_id TEXT NOT NULL,
  payload JSONB,
  PRIMARY KEY(lane,owner_key)
);
CREATE TABLE cdc.v2_producer (
  lane TEXT NOT NULL,
  owner_key TEXT NOT NULL,
  version BIGINT NOT NULL CHECK(version > 0),
  presence TEXT NOT NULL CHECK(presence IN ('PRESENT','DELETED')),
  producer_type TEXT NOT NULL CHECK(producer_type IN ('BASELINE','TRANSACTION')),
  producer_id TEXT NOT NULL,
  final_revision BOOLEAN NOT NULL,
  PRIMARY KEY(lane,owner_key,version)
);
CREATE INDEX v2_producer_transaction ON cdc.v2_producer(lane,producer_id);
CREATE TABLE cdc.v2_receipt (
  lane TEXT NOT NULL,
  transaction_id TEXT NOT NULL,
  mutation_set_hash TEXT NOT NULL,
  mutation_count INTEGER NOT NULL CHECK(mutation_count >= 0),
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(lane,transaction_id),
  FOREIGN KEY(lane,transaction_id) REFERENCES cdc.v2_transaction(lane,transaction_id)
);
CREATE FUNCTION cdc.reject_v2_evidence_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'protocol correctness evidence is immutable' USING ERRCODE='23514'; END $$;
CREATE TRIGGER immutable_receipt BEFORE UPDATE OR DELETE OR TRUNCATE ON cdc.v2_receipt
  FOR EACH STATEMENT EXECUTE FUNCTION cdc.reject_v2_evidence_change();
CREATE TRIGGER immutable_producer BEFORE UPDATE OR DELETE OR TRUNCATE ON cdc.v2_producer
  FOR EACH STATEMENT EXECUTE FUNCTION cdc.reject_v2_evidence_change();
CREATE VIEW cdc.transaction_dependency_graph AS
SELECT t.manifest->>'projectionGeneration' AS projection_generation,
       CASE WHEN d.producer_type='TRANSACTION' THEN d.producer_id END AS predecessor_transaction_id,
       d.transaction_id AS dependent_transaction_id,d.dependency_type,
       d.owner_key::jsonb->>'collection' AS entity_name,d.owner_key,
       d.required_version::text AS required_version,t.state AS dependent_state,
       p.state AS predecessor_state,o.presence AS current_presence,o.version::text AS current_version,
       clock_timestamp() AS as_of
FROM cdc.v2_dependency d
JOIN cdc.v2_transaction t USING(lane,transaction_id)
LEFT JOIN cdc.v2_transaction p ON p.lane=d.lane AND p.transaction_id=d.producer_id AND d.producer_type='TRANSACTION'
LEFT JOIN cdc.v2_owner_state o ON o.lane=d.lane AND o.owner_key=d.owner_key;
