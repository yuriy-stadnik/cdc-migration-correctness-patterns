CREATE TABLE IF NOT EXISTS inventory.customers (
  id BIGINT PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory.accounts (
  account_id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  account_type TEXT,
  balance DECIMAL(15,2),
  street TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT,
  home_phone TEXT,
  work_phone TEXT,
  mobile_phone TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory.orders_flat (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  order_date TIMESTAMPTZ DEFAULT now(),
  product_name TEXT,
  product_category TEXT,
  unit_price DECIMAL(10,2),
  quantity INTEGER,
  total_price DECIMAL(10,2)
);

ALTER TABLE inventory.accounts
  DROP CONSTRAINT IF EXISTS "fk__inventory.accounts__inventory.customers",
  ADD CONSTRAINT "fk__inventory.accounts__inventory.customers"
    FOREIGN KEY (customer_id) REFERENCES inventory.customers(id);

ALTER TABLE inventory.orders_flat
  DROP CONSTRAINT IF EXISTS "fk__inventory.orders_flat__inventory.customers",
  ADD CONSTRAINT "fk__inventory.orders_flat__inventory.customers"
    FOREIGN KEY (customer_id) REFERENCES inventory.customers(id);

ALTER TABLE inventory.customers REPLICA IDENTITY FULL;
ALTER TABLE inventory.accounts REPLICA IDENTITY FULL;
ALTER TABLE inventory.orders_flat REPLICA IDENTITY FULL;

ALTER TABLE inventory.customers OWNER TO dbz;
ALTER TABLE inventory.accounts OWNER TO dbz;
ALTER TABLE inventory.orders_flat OWNER TO dbz;

INSERT INTO inventory.customers (id, first_name, last_name, email, status)
VALUES
  (1, 'Alice', 'Smith', 'alice@example.com', 'ACTIVE'),
  (2, 'Bob', 'Brown', 'bob@example.com', 'ACTIVE')
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory.accounts (
  account_id, customer_id, account_type, balance, street, city, state,
  zip_code, home_phone, work_phone, mobile_phone
)
VALUES
  (101, 1, 'SAVINGS', 5500.00, '123 Maple St', 'Ridgewood', 'NJ', '07450', '201-555-0123', NULL, '201-555-4567'),
  (102, 2, 'CHECKING', 1200.50, '456 Oak Ave', 'Paramus', 'NJ', '07652', '201-555-9999', NULL, NULL)
ON CONFLICT (account_id) DO UPDATE
SET account_type = EXCLUDED.account_type,
    balance = EXCLUDED.balance,
    street = EXCLUDED.street,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    zip_code = EXCLUDED.zip_code,
    home_phone = EXCLUDED.home_phone,
    work_phone = EXCLUDED.work_phone,
    mobile_phone = EXCLUDED.mobile_phone;

INSERT INTO inventory.orders_flat (
  order_id, customer_id, product_name, product_category, unit_price,
  quantity, total_price
)
VALUES
  (5001, 1, 'Wireless Headphones', 'Electronics', 150.00, 1, 150.00),
  (5002, 1, 'USB-C Cable', 'Electronics', 25.00, 2, 50.00),
  (5003, 2, 'Wireless Headphones', 'Electronics', 150.00, 1, 150.00)
ON CONFLICT (order_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    product_category = EXCLUDED.product_category,
    unit_price = EXCLUDED.unit_price,
    quantity = EXCLUDED.quantity,
    total_price = EXCLUDED.total_price;
