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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
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

    @FunctionalInterface
    interface BusinessWrite {
        void apply() throws Exception;
    }

    record ChildDependency(
            String fkName,
            String childSchema,
            String childTable,
            String parentSchema,
            String parentTable
    ) {}

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
            consumer.subscribe(topics);

            while (true) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                    if (record.value() == null || record.value().isBlank()) {
                        continue;
                    }

                    Map<String, Object> payload = MAPPER.readValue(record.value(), MAP_TYPE);
                    try {
                        upsert(clientConn, operationalConn, record.topic(), record.partition(), record.offset(), payload);
                        consumer.commitSync();
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
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
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

    static void upsert(Connection clientConn, Connection operationalConn, String topic, int partition, long offset, Map<String, Object> payload) throws Exception {
        switch (topic) {
            case "pg1.transaction" -> {
                upsertTransactionMetadata(clientConn, payload);
                upsertTransactionMetadata(operationalConn, payload);
            }
            case "client.customers" -> processBusinessEvent(
                    clientConn, topic, String.valueOf(requiredLong(payload, "id")), payload,
                    null,
                    () -> upsertCustomer(clientConn, payload),
                    () -> retryPendingChildren(clientConn, "client", "customers")
            );
            case "client.addresses" -> processBusinessEvent(
                    clientConn, topic, businessKey(
                            "customer_id", requiredLong(payload, "customer_id"),
                            "address_type", requiredText(payload, "address_type")
                    ), payload,
                    new ChildDependency(
                            "fk__client.addresses__client.customers",
                            "client",
                            "addresses",
                            "client",
                            "customers"
                    ),
                    () -> upsertAddress(clientConn, payload),
                    null
            );
            case "operational.products" -> processBusinessEvent(
                    operationalConn, topic, requiredText(payload, "name"), payload,
                    null,
                    () -> upsertProduct(operationalConn, payload),
                    () -> retryPendingChildren(operationalConn, "operational", "products")
            );
            case "operational.orders" -> processBusinessEvent(
                    operationalConn, topic, String.valueOf(requiredLong(payload, "id")), payload,
                    null,
                    () -> upsertOrder(operationalConn, payload),
                    () -> retryPendingChildren(operationalConn, "operational", "orders")
            );
            case "operational.order_items" -> processBusinessEvent(
                    operationalConn, topic, businessKey(
                            "order_id", requiredLong(payload, "order_id"),
                            "product_name", requiredText(payload, "product_name")
                    ), payload,
                    new ChildDependency(
                            "fk__operational.order_items__operational.orders",
                            "operational",
                            "order_items",
                            "operational",
                            "orders"
                    ),
                    () -> upsertOrderItem(operationalConn, payload),
                    null
            );
            case "operational.contact_numbers" -> processBusinessEvent(
                    operationalConn, topic, businessKey(
                            "customer_id", requiredLong(payload, "customer_id"),
                            "phone_type", requiredText(payload, "phone_type")
                    ), payload,
                    null,
                    () -> upsertContactNumber(operationalConn, payload),
                    null
            );
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
                WHERE client.customers.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order >= client.customers.source_tx_total_order
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
                WHERE operational.products.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order >= operational.products.source_tx_total_order
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
                WHERE operational.orders.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order >= operational.orders.source_tx_total_order
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
                WHERE operational.order_items.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order >= operational.order_items.source_tx_total_order
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
                WHERE client.addresses.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order >= client.addresses.source_tx_total_order
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
                WHERE operational.contact_numbers.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order IS NULL
                   OR EXCLUDED.source_tx_total_order >= operational.contact_numbers.source_tx_total_order
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

    static void processBusinessEvent(
            Connection conn,
            String topic,
            String targetBusinessKey,
            Map<String, Object> payload,
            ChildDependency dependency,
            BusinessWrite write,
            BusinessWrite afterWrite
    ) throws Exception {
        boolean originalAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(false);
            if (insertProcessedEvent(conn, topic, targetBusinessKey, payload)) {
                Savepoint businessSavepoint = conn.setSavepoint("business_write");
                try {
                    write.apply();
                    conn.releaseSavepoint(businessSavepoint);
                } catch (SQLException e) {
                    if (!isForeignKeyViolation(e) || dependency == null) {
                        throw e;
                    }
                    conn.rollback(businessSavepoint);
                    postponeForeignKeyEvent(conn, topic, targetBusinessKey, payload, resolveDependency(topic, e, dependency), e);
                }
            }
            if (afterWrite != null) {
                afterWrite.apply();
            }
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(originalAutoCommit);
        }
    }

    static void processBusinessEvent(
            Connection conn,
            String topic,
            String targetBusinessKey,
            Map<String, Object> payload,
            BusinessWrite write
    ) throws Exception {
        processBusinessEvent(conn, topic, targetBusinessKey, payload, null, write, null);
    }

    static boolean insertProcessedEvent(Connection conn, String topic, String targetBusinessKey, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO cdc.processed_events (
                    event_id, source_tx_id, source_tx_total_order, target_topic, target_business_key, payload
                )
                VALUES (?, ?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (event_id) DO NOTHING
                """)) {
            ps.setString(1, idempotencyKey(payload, topic, targetBusinessKey));
            ps.setString(2, processedSourceTxId(payload));
            setNullableLong(ps, 3, optionalLong(payload, "source_tx_total_order"));
            ps.setString(4, topic);
            ps.setString(5, targetBusinessKey);
            ps.setString(6, MAPPER.writeValueAsString(payload));
            return ps.executeUpdate() == 1;
        }
    }

    static void postponeForeignKeyEvent(
            Connection conn,
            String topic,
            String targetBusinessKey,
            Map<String, Object> payload,
            ChildDependency dependency,
            SQLException error
    ) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO cdc.postponed_fk_events (
                    event_id, fk_name, child_schema, child_table, parent_schema, parent_table,
                    target_topic, target_business_key, payload, error_message, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, NOW())
                ON CONFLICT (event_id) DO UPDATE
                SET fk_name = EXCLUDED.fk_name,
                    child_schema = EXCLUDED.child_schema,
                    child_table = EXCLUDED.child_table,
                    parent_schema = EXCLUDED.parent_schema,
                    parent_table = EXCLUDED.parent_table,
                    target_topic = EXCLUDED.target_topic,
                    target_business_key = EXCLUDED.target_business_key,
                    payload = EXCLUDED.payload,
                    error_message = EXCLUDED.error_message,
                    status = 'PENDING',
                    updated_at = NOW()
                """)) {
            ps.setString(1, idempotencyKey(payload, topic, targetBusinessKey));
            ps.setString(2, dependency.fkName());
            ps.setString(3, dependency.childSchema());
            ps.setString(4, dependency.childTable());
            ps.setString(5, dependency.parentSchema());
            ps.setString(6, dependency.parentTable());
            ps.setString(7, topic);
            ps.setString(8, targetBusinessKey);
            ps.setString(9, MAPPER.writeValueAsString(payload));
            ps.setString(10, error.getMessage());
            ps.executeUpdate();
        }
    }

    static void retryPendingChildren(Connection conn, String parentSchema, String parentTable) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT event_id, target_topic, payload::text
                FROM cdc.postponed_fk_events
                WHERE status = 'PENDING'
                  AND parent_schema = ?
                  AND parent_table = ?
                ORDER BY created_at
                FOR UPDATE
                """)) {
            ps.setString(1, parentSchema);
            ps.setString(2, parentTable);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String eventId = rs.getString("event_id");
                    String topic = rs.getString("target_topic");
                    Map<String, Object> payload = MAPPER.readValue(rs.getString("payload"), MAP_TYPE);
                    retryPendingChild(conn, eventId, topic, payload);
                }
            }
        }
    }

    static void retryPendingChild(Connection conn, String eventId, String topic, Map<String, Object> payload) throws Exception {
        Savepoint retrySavepoint = conn.setSavepoint("retry_pending_child");
        try {
            upsertPendingChild(topic, conn, payload);
            markPendingChildApplied(conn, eventId);
            conn.releaseSavepoint(retrySavepoint);
        } catch (SQLException e) {
            if (!isForeignKeyViolation(e)) {
                throw e;
            }
            conn.rollback(retrySavepoint);
            markPendingChildRetry(conn, eventId, resolveDependency(topic, e, fallbackDependency(topic)), e);
        }
    }

    static void upsertPendingChild(String topic, Connection conn, Map<String, Object> payload) throws Exception {
        switch (topic) {
            case "client.addresses" -> upsertAddress(conn, payload);
            case "operational.order_items" -> upsertOrderItem(conn, payload);
            default -> throw new IllegalArgumentException("Unsupported postponed FK topic: " + topic);
        }
    }

    static void markPendingChildApplied(Connection conn, String eventId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE cdc.postponed_fk_events
                SET status = 'APPLIED',
                    updated_at = NOW(),
                    last_retry_at = NOW()
                WHERE event_id = ?
                """)) {
            ps.setString(1, eventId);
            ps.executeUpdate();
        }
    }

    static void markPendingChildRetry(Connection conn, String eventId, ChildDependency dependency, SQLException error) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE cdc.postponed_fk_events
                SET retry_count = retry_count + 1,
                    fk_name = ?,
                    child_schema = ?,
                    child_table = ?,
                    parent_schema = ?,
                    parent_table = ?,
                    error_message = ?,
                    updated_at = NOW(),
                    last_retry_at = NOW()
                WHERE event_id = ?
                """)) {
            ps.setString(1, dependency.fkName());
            ps.setString(2, dependency.childSchema());
            ps.setString(3, dependency.childTable());
            ps.setString(4, dependency.parentSchema());
            ps.setString(5, dependency.parentTable());
            ps.setString(6, error.getMessage());
            ps.setString(7, eventId);
            ps.executeUpdate();
        }
    }

    static boolean isForeignKeyViolation(SQLException error) {
        SQLException current = error;
        while (current != null) {
            if ("23503".equals(current.getSQLState())) {
                return true;
            }
            current = current.getNextException();
        }
        return false;
    }

    static ChildDependency resolveDependency(String topic, SQLException error, ChildDependency fallback) {
        String fkName = foreignKeyName(error);
        if (fkName == null || !fkName.startsWith("fk__")) {
            return fallback;
        }
        String[] sides = fkName.substring(4).split("__", 2);
        if (sides.length != 2) {
            return fallback;
        }
        String[] child = sides[0].split("[.]", 2);
        String[] parent = sides[1].split("[.]", 2);
        if (child.length != 2 || parent.length != 2) {
            return fallback;
        }
        return new ChildDependency(fkName, child[0], child[1], parent[0], parent[1]);
    }

    static ChildDependency fallbackDependency(String topic) {
        return switch (topic) {
            case "client.addresses" -> new ChildDependency(
                    "fk__client.addresses__client.customers",
                    "client",
                    "addresses",
                    "client",
                    "customers"
            );
            case "operational.order_items" -> new ChildDependency(
                    "fk__operational.order_items__operational.orders",
                    "operational",
                    "order_items",
                    "operational",
                    "orders"
            );
            default -> new ChildDependency("unknown", "unknown", "unknown", "unknown", "unknown");
        };
    }

    static String foreignKeyName(SQLException error) {
        SQLException current = error;
        while (current != null) {
            String fkName = foreignKeyName(current.getMessage());
            if (fkName != null) {
                return fkName;
            }
            current = current.getNextException();
        }
        return null;
    }

    static String foreignKeyName(String message) {
        if (message == null) {
            return null;
        }
        String marker = "foreign key constraint \"";
        int start = message.indexOf(marker);
        if (start < 0) {
            return null;
        }
        int nameStart = start + marker.length();
        int nameEnd = message.indexOf('"', nameStart);
        return nameEnd < 0 ? null : message.substring(nameStart, nameEnd);
    }

    static String idempotencyKey(Map<String, Object> payload, String targetTopic, String targetBusinessKey) {
        String upstreamKey = optionalText(payload, "idempotency_key");
        if (upstreamKey != null && !upstreamKey.isBlank()) {
            return upstreamKey;
        }
        Long sourceTxTotalOrder = optionalLong(payload, "source_tx_total_order");
        return "%s|%s|%s|%s".formatted(
                processedSourceTxId(payload),
                sourceTxTotalOrder == null ? -1L : sourceTxTotalOrder,
                targetTopic,
                targetBusinessKey
        );
    }

    static String processedSourceTxId(Map<String, Object> payload) {
        String sourceTxId = optionalText(payload, "source_tx_id");
        return sourceTxId == null || sourceTxId.isBlank() ? "no-tx" : sourceTxId;
    }

    static String businessKey(String firstName, Object firstValue, String secondName, Object secondValue) {
        return "%s=%s|%s=%s".formatted(firstName, firstValue, secondName, secondValue);
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
