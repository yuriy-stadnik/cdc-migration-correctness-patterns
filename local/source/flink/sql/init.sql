SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE customers_cdc (
  `before` ROW<
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    status STRING,
    created_at STRING,
    updated_at STRING
  >,
  `after` ROW<
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    status STRING,
    created_at STRING,
    updated_at STRING
  >,
  op STRING,
  ts_ms BIGINT,
  `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg1.inventory.customers',
  'properties.bootstrap.servers' = 'source-kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE accounts_cdc (
  `before` ROW<
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
    created_at STRING
  >,
  `after` ROW<
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
    created_at STRING
  >,
  op STRING,
  ts_ms BIGINT,
  `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg1.inventory.accounts',
  'properties.bootstrap.servers' = 'source-kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE orders_flat_cdc (
  `before` ROW<
    order_id BIGINT,
    customer_id BIGINT,
    order_date STRING,
    product_name STRING,
    product_category STRING,
    unit_price DECIMAL(10,2),
    quantity INT,
    total_price DECIMAL(10,2)
  >,
  `after` ROW<
    order_id BIGINT,
    customer_id BIGINT,
    order_date STRING,
    product_name STRING,
    product_category STRING,
    unit_price DECIMAL(10,2),
    quantity INT,
    total_price DECIMAL(10,2)
  >,
  op STRING,
  ts_ms BIGINT,
  `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg1.inventory.orders_flat',
  'properties.bootstrap.servers' = 'source-kafka:29092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE client_customers_topic (
  id BIGINT,
  first_name STRING,
  last_name STRING,
  email STRING,
  status STRING,
  created_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'client.customers',
  'properties.bootstrap.servers' = 'source-kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE client_addresses_topic (
  customer_id BIGINT,
  address_type STRING,
  street STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (customer_id, address_type) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'client.addresses',
  'properties.bootstrap.servers' = 'source-kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_products_topic (
  name STRING,
  category STRING,
  current_price DECIMAL(10,2),
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (name) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.products',
  'properties.bootstrap.servers' = 'source-kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_orders_topic (
  id BIGINT,
  customer_id BIGINT,
  order_date TIMESTAMP(3),
  status STRING,
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.orders',
  'properties.bootstrap.servers' = 'source-kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_order_items_topic (
  order_id BIGINT,
  product_name STRING,
  quantity INT,
  price_at_purchase DECIMAL(10,2),
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (order_id, product_name) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.order_items',
  'properties.bootstrap.servers' = 'source-kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE operational_contact_numbers_topic (
  customer_id BIGINT,
  phone_type STRING,
  phone_number STRING,
  source_record_type STRING,
  source_ts_ms BIGINT,
  source_tx_id STRING,
  source_tx_total_order BIGINT,
  source_tx_data_collection_order BIGINT,
  PRIMARY KEY (customer_id, phone_type) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'operational.contact_numbers',
  'properties.bootstrap.servers' = 'source-kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO client_customers_topic
SELECT
  COALESCE(`after`.id, `before`.id) AS id,
  COALESCE(`after`.first_name, `before`.first_name) AS first_name,
  COALESCE(`after`.last_name, `before`.last_name) AS last_name,
  COALESCE(`after`.email, `before`.email) AS email,
  COALESCE(`after`.status, `before`.status) AS status,
  CAST(REPLACE(SUBSTRING(COALESCE(`after`.created_at, `before`.created_at), 1, 19), 'T', ' ') AS TIMESTAMP(3)) AS created_at,
  CAST(REPLACE(SUBSTRING(COALESCE(`after`.updated_at, `before`.updated_at), 1, 19), 'T', ' ') AS TIMESTAMP(3)) AS updated_at,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM customers_cdc
WHERE COALESCE(`after`.id, `before`.id) IS NOT NULL;

INSERT INTO client_addresses_topic
SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  'HOME' AS address_type,
  COALESCE(`after`.street, `before`.street) AS street,
  COALESCE(`after`.city, `before`.city) AS city,
  COALESCE(`after`.state, `before`.state) AS state,
  COALESCE(`after`.zip_code, `before`.zip_code) AS zip_code,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM accounts_cdc
WHERE COALESCE(`after`.street, `before`.street) IS NOT NULL;

INSERT INTO operational_contact_numbers_topic
SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  'HOME' AS phone_type,
  COALESCE(`after`.home_phone, `before`.home_phone) AS phone_number,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM accounts_cdc
WHERE COALESCE(`after`.home_phone, `before`.home_phone) IS NOT NULL;

INSERT INTO operational_contact_numbers_topic
SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  'WORK' AS phone_type,
  COALESCE(`after`.work_phone, `before`.work_phone) AS phone_number,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM accounts_cdc
WHERE COALESCE(`after`.work_phone, `before`.work_phone) IS NOT NULL;

INSERT INTO operational_contact_numbers_topic
SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  'MOBILE' AS phone_type,
  COALESCE(`after`.mobile_phone, `before`.mobile_phone) AS phone_number,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM accounts_cdc
WHERE COALESCE(`after`.mobile_phone, `before`.mobile_phone) IS NOT NULL;

INSERT INTO operational_products_topic
SELECT
  COALESCE(`after`.product_name, `before`.product_name) AS name,
  COALESCE(`after`.product_category, `before`.product_category) AS category,
  COALESCE(`after`.unit_price, `before`.unit_price) AS current_price,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM orders_flat_cdc
WHERE COALESCE(`after`.product_name, `before`.product_name) IS NOT NULL;

INSERT INTO operational_orders_topic
SELECT
  COALESCE(`after`.order_id, `before`.order_id) AS id,
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  CAST(REPLACE(SUBSTRING(COALESCE(`after`.order_date, `before`.order_date), 1, 19), 'T', ' ') AS TIMESTAMP(3)) AS order_date,
  'COMPLETED' AS status,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM orders_flat_cdc
WHERE COALESCE(`after`.order_id, `before`.order_id) IS NOT NULL;

INSERT INTO operational_order_items_topic
SELECT
  COALESCE(`after`.order_id, `before`.order_id) AS order_id,
  COALESCE(`after`.product_name, `before`.product_name) AS product_name,
  COALESCE(`after`.quantity, `before`.quantity) AS quantity,
  COALESCE(`after`.unit_price, `before`.unit_price) AS price_at_purchase,
  CASE op WHEN 'c' THEN 'I' WHEN 'r' THEN 'I' WHEN 'u' THEN 'U' WHEN 'd' THEN 'D' ELSE op END AS source_record_type,
  ts_ms AS source_ts_ms,
  `transaction`.id AS source_tx_id,
  `transaction`.total_order AS source_tx_total_order,
  `transaction`.data_collection_order AS source_tx_data_collection_order
FROM orders_flat_cdc
WHERE COALESCE(`after`.product_name, `before`.product_name) IS NOT NULL;
