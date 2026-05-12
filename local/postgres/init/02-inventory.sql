CREATE TABLE IF NOT EXISTS inventory.customers (
  id BIGINT PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE inventory.customers REPLICA IDENTITY FULL;
ALTER TABLE inventory.customers OWNER TO dbz;

INSERT INTO inventory.customers (id, first_name, last_name, email, status)
VALUES
  (1, 'Alice', 'Smith', 'alice@example.com', 'ACTIVE'),
  (2, 'Bob', 'Brown', 'bob@example.com', 'ACTIVE')
ON CONFLICT (id) DO NOTHING;
