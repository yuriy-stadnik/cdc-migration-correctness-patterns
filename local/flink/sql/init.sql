SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE customers_cdc (
  id BIGINT,
  first_name STRING,
  last_name STRING,
  email STRING,
  status STRING,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg1.inventory.customers',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE accounts_cdc (
  account_id BIGINT,
  customer_id BIGINT,
  account_type STRING,
  balance DECIMAL(15,2),
  street STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  home_phone STRING,
  work_phone STRING,
  mobile_phone STRING,
  created_at STRING,
  PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg1.inventory.accounts',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE orders_flat_cdc (
  order_id BIGINT,
  customer_id BIGINT,
  order_date STRING,
  product_name STRING,
  product_category STRING,
  unit_price DECIMAL(10,2),
  quantity INT,
  total_price DECIMAL(10,2),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg1.inventory.orders_flat',
  'properties.bootstrap.servers' = 'kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE customer_projection (
  id BIGINT,
  email STRING,
  full_name STRING,
  status STRING,
  source_system STRING,
  source_table STRING,
  source_lsn STRING,
  source_tx_id STRING,
  source_event_time STRING,
  ingested_at STRING,
  schema_version STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://target-postgres:5432/microservices',
  'table-name' = 'customer_projection',
  'username' = 'appuser',
  'password' = 'apppass'
);

CREATE TABLE operational_addresses (
  customer_id BIGINT,
  address_type STRING,
  street STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  PRIMARY KEY (customer_id, address_type) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://target-postgres:5432/microservices',
  'table-name' = 'operational.addresses',
  'username' = 'appuser',
  'password' = 'apppass'
);

CREATE TABLE operational_contact_numbers (
  customer_id BIGINT,
  phone_type STRING,
  phone_number STRING,
  PRIMARY KEY (customer_id, phone_type) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://target-postgres:5432/microservices',
  'table-name' = 'operational.contact_numbers',
  'username' = 'appuser',
  'password' = 'apppass'
);

CREATE TABLE operational_products (
  name STRING,
  category STRING,
  current_price DECIMAL(10,2),
  PRIMARY KEY (name) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://target-postgres:5432/microservices',
  'table-name' = 'operational.products',
  'username' = 'appuser',
  'password' = 'apppass'
);

CREATE TABLE operational_orders (
  id BIGINT,
  customer_id BIGINT,
  order_date TIMESTAMP(3),
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://target-postgres:5432/microservices',
  'table-name' = 'operational.orders',
  'username' = 'appuser',
  'password' = 'apppass'
);

CREATE TABLE operational_order_items (
  order_id BIGINT,
  product_name STRING,
  quantity INT,
  price_at_purchase DECIMAL(10,2),
  PRIMARY KEY (order_id, product_name) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://target-postgres:5432/microservices',
  'table-name' = 'operational.order_items',
  'username' = 'appuser',
  'password' = 'apppass'
);

CREATE TABLE operational_products_topic (
  name STRING,
  category STRING,
  current_price DECIMAL(10,2),
  PRIMARY KEY (name) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.products',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_orders_topic (
  id BIGINT,
  customer_id BIGINT,
  order_date TIMESTAMP(3),
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.orders',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_order_items_topic (
  order_id BIGINT,
  product_name STRING,
  quantity INT,
  price_at_purchase DECIMAL(10,2),
  PRIMARY KEY (order_id, product_name) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.order_items',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_addresses_topic (
  customer_id BIGINT,
  address_type STRING,
  street STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  PRIMARY KEY (customer_id, address_type) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.addresses',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_contact_numbers_topic (
  customer_id BIGINT,
  phone_type STRING,
  phone_number STRING,
  PRIMARY KEY (customer_id, phone_type) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.contact_numbers',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO customer_projection
SELECT
  id,
  email,
  CONCAT(first_name, ' ', last_name) AS full_name,
  COALESCE(status, 'ACTIVE') AS status,
  'legacy-postgres' AS source_system,
  'inventory.customers' AS source_table,
  CAST(NULL AS STRING) AS source_lsn,
  CAST(NULL AS STRING) AS source_tx_id,
  CAST(NULL AS STRING) AS source_event_time,
  CAST(CURRENT_TIMESTAMP AS STRING) AS ingested_at,
  'v1' AS schema_version
FROM customers_cdc;

INSERT INTO operational_addresses
SELECT
  customer_id,
  'HOME' AS address_type,
  street,
  city,
  state,
  zip_code
FROM accounts_cdc
WHERE street IS NOT NULL;

INSERT INTO operational_contact_numbers
SELECT customer_id, 'HOME' AS phone_type, home_phone AS phone_number
FROM accounts_cdc
WHERE home_phone IS NOT NULL;

INSERT INTO operational_contact_numbers
SELECT customer_id, 'WORK' AS phone_type, work_phone AS phone_number
FROM accounts_cdc
WHERE work_phone IS NOT NULL;

INSERT INTO operational_contact_numbers
SELECT customer_id, 'MOBILE' AS phone_type, mobile_phone AS phone_number
FROM accounts_cdc
WHERE mobile_phone IS NOT NULL;

INSERT INTO operational_products
SELECT
  product_name AS name,
  product_category AS category,
  unit_price AS current_price
FROM orders_flat_cdc
WHERE product_name IS NOT NULL;

INSERT INTO operational_addresses_topic
SELECT
  customer_id,
  'HOME' AS address_type,
  street,
  city,
  state,
  zip_code
FROM accounts_cdc
WHERE street IS NOT NULL;

INSERT INTO operational_contact_numbers_topic
SELECT customer_id, 'HOME' AS phone_type, home_phone AS phone_number
FROM accounts_cdc
WHERE home_phone IS NOT NULL;

INSERT INTO operational_contact_numbers_topic
SELECT customer_id, 'WORK' AS phone_type, work_phone AS phone_number
FROM accounts_cdc
WHERE work_phone IS NOT NULL;

INSERT INTO operational_contact_numbers_topic
SELECT customer_id, 'MOBILE' AS phone_type, mobile_phone AS phone_number
FROM accounts_cdc
WHERE mobile_phone IS NOT NULL;

INSERT INTO operational_products_topic
SELECT
  product_name AS name,
  product_category AS category,
  unit_price AS current_price
FROM orders_flat_cdc
WHERE product_name IS NOT NULL;

INSERT INTO operational_orders_topic
SELECT
  order_id AS id,
  customer_id,
  CAST(REPLACE(SUBSTRING(order_date, 1, 19), 'T', ' ') AS TIMESTAMP(3)) AS order_date,
  'COMPLETED' AS status
FROM orders_flat_cdc;

INSERT INTO operational_order_items_topic
SELECT
  order_id,
  product_name,
  quantity,
  unit_price AS price_at_purchase
FROM orders_flat_cdc
WHERE product_name IS NOT NULL;
INSERT INTO operational_orders
SELECT
  order_id AS id,
  customer_id,
  CAST(REPLACE(SUBSTRING(order_date, 1, 19), 'T', ' ') AS TIMESTAMP(3)) AS order_date,
  'COMPLETED' AS status
FROM orders_flat_cdc;

INSERT INTO operational_order_items
SELECT
  order_id,
  product_name,
  quantity,
  unit_price AS price_at_purchase
FROM orders_flat_cdc
WHERE product_name IS NOT NULL;
