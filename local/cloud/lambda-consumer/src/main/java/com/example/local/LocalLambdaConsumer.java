package com.example.local;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class LocalLambdaConsumer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final List<String> topics = Arrays.stream(requiredEnv("KAFKA_TOPICS").split(","))
            .map(String::trim)
            .filter(topic -> !topic.isBlank())
            .collect(Collectors.toList());
    private final String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "cloud-kafka:39092");
    private final String groupId = env("KAFKA_GROUP_ID", "local-lambda-to-postgres");
    private final String clientJdbcUrl = "jdbc:postgresql://%s:%s/%s".formatted(
            requiredEnv("CLIENT_DB_HOST"),
            env("CLIENT_DB_PORT", "5432"),
            requiredEnv("CLIENT_DB_NAME")
    );
    private final String clientDbUser = requiredEnv("CLIENT_DB_USER");
    private final String clientDbPassword = requiredEnv("CLIENT_DB_PASSWORD");
    private final String operationalJdbcUrl = "jdbc:postgresql://%s:%s/%s".formatted(
            requiredEnv("OPERATIONAL_DB_HOST"),
            env("OPERATIONAL_DB_PORT", "5432"),
            requiredEnv("OPERATIONAL_DB_NAME")
    );
    private final String operationalDbUser = requiredEnv("OPERATIONAL_DB_USER");
    private final String operationalDbPassword = requiredEnv("OPERATIONAL_DB_PASSWORD");

    public static void main(String[] args) throws Exception {
        new LocalLambdaConsumer().run();
    }

    private void run() throws Exception {
        System.out.printf("Starting local Lambda replacement for topics: %s%n", String.join(", ", topics));

        try (
                Connection clientConn = waitForDb("client", clientJdbcUrl, clientDbUser, clientDbPassword);
                Connection operationalConn = waitForDb("operational", operationalJdbcUrl, operationalDbUser, operationalDbPassword);
                KafkaConsumer<String, String> consumer = waitForConsumer()
        ) {
            ensureClientSchema(clientConn);
            ensureOperationalSchema(operationalConn);
            consumer.subscribe(topics);

            while (true) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                    if (record.value() == null || record.value().isBlank()) {
                        continue;
                    }

                    Map<String, Object> payload = MAPPER.readValue(record.value(), MAP_TYPE);
                    try {
                        upsert(clientConn, operationalConn, record.topic(), record.partition(), record.offset(), payload);
                        System.out.printf("processed %s partition=%d offset=%d%n",
                                record.topic(), record.partition(), record.offset());
                    } catch (Exception e) {
                        System.err.printf("failed %s partition=%d offset=%d: %s%n",
                                record.topic(), record.partition(), record.offset(), e.getMessage());
                        throw e;
                    }
                }
            }
        }
    }

    private Connection waitForDb(String name, String jdbcUrl, String user, String password) throws InterruptedException {
        while (true) {
            try {
                Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                conn.setAutoCommit(true);
                return conn;
            } catch (Exception e) {
                System.out.printf("Waiting for %s database: %s%n", name, e.getMessage());
                Thread.sleep(3000);
            }
        }
    }

    private KafkaConsumer<String, String> waitForConsumer() throws InterruptedException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        while (true) {
            try {
                return new KafkaConsumer<>(props);
            } catch (Exception e) {
                System.out.printf("Waiting for cloud-kafka: %s%n", e.getMessage());
                Thread.sleep(3000);
            }
        }
    }

    private void ensureClientSchema(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("""
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

                    ALTER TABLE client.customers
                      ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);

                    ALTER TABLE client.addresses
                      ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);

                    CREATE TABLE IF NOT EXISTS cdc.transaction_metadata (
                      tx_id TEXT NOT NULL,
                      status TEXT NOT NULL,
                      event_count BIGINT,
                      data_collections JSONB,
                      ts_ms BIGINT,
                      updated_at TIMESTAMPTZ DEFAULT NOW(),
                      PRIMARY KEY (tx_id, status)
                    );
                    """);
        }
    }

    private void ensureOperationalSchema(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("""
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

                    ALTER TABLE operational.products
                      ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);

                    ALTER TABLE operational.orders
                      ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);

                    ALTER TABLE operational.order_items
                      ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);

                    ALTER TABLE operational.contact_numbers
                      ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);

                    CREATE TABLE IF NOT EXISTS cdc.transaction_metadata (
                      tx_id TEXT NOT NULL,
                      status TEXT NOT NULL,
                      event_count BIGINT,
                      data_collections JSONB,
                      ts_ms BIGINT,
                      updated_at TIMESTAMPTZ DEFAULT NOW(),
                      PRIMARY KEY (tx_id, status)
                    );
                    """);
        }
    }

    static void upsert(Connection clientConn, Connection operationalConn, String topic, int partition, long offset, Map<String, Object> payload) throws Exception {
        switch (topic) {
            case "pg1.transaction" -> {
                upsertTransactionMetadata(clientConn, payload);
                upsertTransactionMetadata(operationalConn, payload);
            }
            case "client.customers" -> upsertCustomer(clientConn, payload);
            case "client.addresses" -> upsertAddress(clientConn, payload);
            case "operational.products" -> upsertProduct(operationalConn, payload);
            case "operational.orders" -> upsertOrder(operationalConn, payload);
            case "operational.order_items" -> upsertOrderItem(operationalConn, payload);
            case "operational.contact_numbers" -> upsertContactNumber(operationalConn, payload);
            default -> {
                Connection eventConn = topic.startsWith("client.") ? clientConn : operationalConn;
                insertRawEvent(eventConn, topic, partition, offset, payload);
            }
        }
    }

    static void upsertCustomer(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO client.customers (
                    id, first_name, last_name, email, status, created_at, updated_at,
                    source_record_type, source_ts_ms, source_tx_id, source_tx_total_order,
                    source_tx_data_collection_order, idempotency_key
                )
                VALUES (?, ?, ?, ?, ?, ?::timestamptz, ?::timestamptz, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE
                SET first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    status = EXCLUDED.status,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    source_record_type = EXCLUDED.source_record_type,
                    source_ts_ms = EXCLUDED.source_ts_ms,
                    source_tx_id = EXCLUDED.source_tx_id,
                    source_tx_total_order = EXCLUDED.source_tx_total_order,
                    source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order,
                    idempotency_key = EXCLUDED.idempotency_key
                """)) {
            ps.setLong(1, requiredLong(payload, "id"));
            ps.setString(2, requiredText(payload, "first_name"));
            ps.setString(3, requiredText(payload, "last_name"));
            ps.setString(4, optionalText(payload, "email"));
            ps.setString(5, optionalText(payload, "status"));
            ps.setString(6, optionalText(payload, "created_at"));
            ps.setString(7, optionalText(payload, "updated_at"));
            setSourceMetadata(ps, 8, payload);
            ps.executeUpdate();
        }
    }

    static void upsertProduct(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO operational.products (
                    name, category, current_price, source_record_type, source_ts_ms, source_tx_id,
                    source_tx_total_order, source_tx_data_collection_order, idempotency_key
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (name) DO UPDATE
                SET category = EXCLUDED.category,
                    current_price = EXCLUDED.current_price,
                    source_record_type = EXCLUDED.source_record_type,
                    source_ts_ms = EXCLUDED.source_ts_ms,
                    source_tx_id = EXCLUDED.source_tx_id,
                    source_tx_total_order = EXCLUDED.source_tx_total_order,
                    source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order,
                    idempotency_key = EXCLUDED.idempotency_key
                """)) {
            ps.setString(1, requiredText(payload, "name"));
            ps.setString(2, optionalText(payload, "category"));
            ps.setBigDecimal(3, optionalDecimal(payload, "current_price"));
            setSourceMetadata(ps, 4, payload);
            ps.executeUpdate();
        }
    }

    static void upsertOrder(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO operational.orders (
                    id, customer_id, order_date, status, source_record_type, source_ts_ms,
                    source_tx_id, source_tx_total_order, source_tx_data_collection_order, idempotency_key
                )
                VALUES (?, ?, ?::timestamptz, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE
                SET customer_id = EXCLUDED.customer_id,
                    order_date = EXCLUDED.order_date,
                    status = EXCLUDED.status,
                    source_record_type = EXCLUDED.source_record_type,
                    source_ts_ms = EXCLUDED.source_ts_ms,
                    source_tx_id = EXCLUDED.source_tx_id,
                    source_tx_total_order = EXCLUDED.source_tx_total_order,
                    source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order,
                    idempotency_key = EXCLUDED.idempotency_key
                """)) {
            ps.setLong(1, requiredLong(payload, "id"));
            ps.setLong(2, requiredLong(payload, "customer_id"));
            ps.setString(3, optionalText(payload, "order_date"));
            ps.setString(4, optionalText(payload, "status"));
            setSourceMetadata(ps, 5, payload);
            ps.executeUpdate();
        }
    }

    static void upsertOrderItem(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO operational.order_items (
                    order_id, product_name, quantity, price_at_purchase, source_record_type,
                    source_ts_ms, source_tx_id, source_tx_total_order, source_tx_data_collection_order,
                    idempotency_key
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (order_id, product_name) DO UPDATE
                SET quantity = EXCLUDED.quantity,
                    price_at_purchase = EXCLUDED.price_at_purchase,
                    source_record_type = EXCLUDED.source_record_type,
                    source_ts_ms = EXCLUDED.source_ts_ms,
                    source_tx_id = EXCLUDED.source_tx_id,
                    source_tx_total_order = EXCLUDED.source_tx_total_order,
                    source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order,
                    idempotency_key = EXCLUDED.idempotency_key
                """)) {
            ps.setLong(1, requiredLong(payload, "order_id"));
            ps.setString(2, requiredText(payload, "product_name"));
            ps.setInt(3, requiredInt(payload, "quantity"));
            ps.setBigDecimal(4, optionalDecimal(payload, "price_at_purchase"));
            setSourceMetadata(ps, 5, payload);
            ps.executeUpdate();
        }
    }

    static void upsertAddress(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO client.addresses (
                    customer_id, address_type, street, city, state, zip_code, source_record_type,
                    source_ts_ms, source_tx_id, source_tx_total_order, source_tx_data_collection_order,
                    idempotency_key
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (customer_id, address_type) DO UPDATE
                SET street = EXCLUDED.street,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    source_record_type = EXCLUDED.source_record_type,
                    source_ts_ms = EXCLUDED.source_ts_ms,
                    source_tx_id = EXCLUDED.source_tx_id,
                    source_tx_total_order = EXCLUDED.source_tx_total_order,
                    source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order,
                    idempotency_key = EXCLUDED.idempotency_key
                """)) {
            ps.setLong(1, requiredLong(payload, "customer_id"));
            ps.setString(2, requiredText(payload, "address_type"));
            ps.setString(3, optionalText(payload, "street"));
            ps.setString(4, optionalText(payload, "city"));
            ps.setString(5, optionalText(payload, "state"));
            ps.setString(6, optionalText(payload, "zip_code"));
            setSourceMetadata(ps, 7, payload);
            ps.executeUpdate();
        }
    }

    static void upsertContactNumber(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO operational.contact_numbers (
                    customer_id, phone_type, phone_number, source_record_type, source_ts_ms,
                    source_tx_id, source_tx_total_order, source_tx_data_collection_order, idempotency_key
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (customer_id, phone_type) DO UPDATE
                SET phone_number = EXCLUDED.phone_number,
                    source_record_type = EXCLUDED.source_record_type,
                    source_ts_ms = EXCLUDED.source_ts_ms,
                    source_tx_id = EXCLUDED.source_tx_id,
                    source_tx_total_order = EXCLUDED.source_tx_total_order,
                    source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order,
                    idempotency_key = EXCLUDED.idempotency_key
                """)) {
            ps.setLong(1, requiredLong(payload, "customer_id"));
            ps.setString(2, requiredText(payload, "phone_type"));
            ps.setString(3, requiredText(payload, "phone_number"));
            setSourceMetadata(ps, 4, payload);
            ps.executeUpdate();
        }
    }

    static void upsertTransactionMetadata(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO cdc.transaction_metadata (tx_id, status, event_count, data_collections, ts_ms, updated_at)
                VALUES (?, ?, ?, ?::jsonb, ?, NOW())
                ON CONFLICT (tx_id, status) DO UPDATE
                SET event_count = EXCLUDED.event_count,
                    data_collections = EXCLUDED.data_collections,
                    ts_ms = EXCLUDED.ts_ms,
                    updated_at = NOW()
                """)) {
            ps.setString(1, requiredText(payload, "id"));
            ps.setString(2, requiredText(payload, "status"));
            setNullableLong(ps, 3, optionalLong(payload, "event_count"));
            Object dataCollections = payload.get("data_collections");
            ps.setString(4, dataCollections == null ? "[]" : MAPPER.writeValueAsString(dataCollections));
            setNullableLong(ps, 5, optionalLong(payload, "ts_ms"));
            ps.executeUpdate();
        }
    }

    static void insertRawEvent(Connection conn, String topic, int partition, long offset, Map<String, Object> payload) throws Exception {
        String table = topic.startsWith("client.") ? "client.events" : "operational.events";
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO %s (topic, kafka_partition, kafka_offset, payload)
                VALUES (?, ?, ?, ?::jsonb)
                ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
                """.formatted(table))) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setLong(3, offset);
            ps.setString(4, MAPPER.writeValueAsString(payload));
            ps.executeUpdate();
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable: " + name);
        }
        return value;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    static String requiredText(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return value.toString();
    }

    static String optionalText(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        return value == null ? null : value.toString();
    }

    static long requiredLong(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(requiredText(payload, field));
    }

    static int requiredInt(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(requiredText(payload, field));
    }

    static BigDecimal optionalDecimal(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        return value == null ? null : new BigDecimal(value.toString());
    }

    static void setSourceMetadata(PreparedStatement ps, int start, Map<String, Object> payload) throws Exception {
        ps.setString(start, optionalText(payload, "source_record_type"));
        setNullableLong(ps, start + 1, optionalLong(payload, "source_ts_ms"));
        ps.setString(start + 2, optionalText(payload, "source_tx_id"));
        setNullableLong(ps, start + 3, optionalLong(payload, "source_tx_total_order"));
        setNullableLong(ps, start + 4, optionalLong(payload, "source_tx_data_collection_order"));
        ps.setString(start + 5, optionalText(payload, "idempotency_key"));
    }

    static Long optionalLong(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        if (value == null || value.toString().isBlank()) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    static void setNullableLong(PreparedStatement ps, int index, Long value) throws Exception {
        if (value == null) {
            ps.setObject(index, null);
        } else {
            ps.setLong(index, value);
        }
    }
}
