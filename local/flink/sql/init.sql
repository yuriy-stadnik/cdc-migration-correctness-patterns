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
  'properties.group.id' = 'flink-sql-customers-projection',
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
