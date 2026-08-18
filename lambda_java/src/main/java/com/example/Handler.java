package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Handler implements RequestHandler<KafkaEvent, Map<String, Object>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Object PW_LOCK = new Object();
    private static final Map<String, String> CACHED_PASSWORDS = new HashMap<>();
    private static final AtomicBoolean CLIENT_DDL_ENSURED = new AtomicBoolean(false);
    private static final AtomicBoolean OPERATIONAL_DDL_ENSURED = new AtomicBoolean(false);

    private final String clientDbHost = mustEnv("CLIENT_DB_HOST");
    private final String clientDbPort = env("CLIENT_DB_PORT", "5432");
    private final String clientDbName = mustEnv("CLIENT_DB_NAME");
    private final String clientDbUser = mustEnv("CLIENT_DB_USER");
    private final String clientDbSecretArn = mustEnv("CLIENT_DB_SECRET_ARN");

    private final String operationalDbHost = mustEnv("OPERATIONAL_DB_HOST");
    private final String operationalDbPort = env("OPERATIONAL_DB_PORT", "5432");
    private final String operationalDbName = mustEnv("OPERATIONAL_DB_NAME");
    private final String operationalDbUser = mustEnv("OPERATIONAL_DB_USER");
    private final String operationalDbSecretArn = mustEnv("OPERATIONAL_DB_SECRET_ARN");

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

    @Override
    public Map<String, Object> handleRequest(KafkaEvent event, Context context) {
        Map<String, List<KafkaEvent.KafkaEventRecord>> records = event.getRecords();
        log(context, "Records keys: " + (records == null ? "null" : records.keySet()));

        if (records == null || records.isEmpty()) {
            return Map.of("status", "ok", "processed", 0);
        }

        try (
            Connection clientConn = openConnection(clientDbHost, clientDbPort, clientDbName, clientDbUser, clientDbSecretArn);
            Connection operationalConn = openConnection(operationalDbHost, operationalDbPort, operationalDbName, operationalDbUser, operationalDbSecretArn)
        ) {
            ensureClientSchema(clientConn);
            ensureOperationalSchema(operationalConn);

            int processed = 0;
            int skipped = 0;

            for (Map.Entry<String, List<KafkaEvent.KafkaEventRecord>> entry : records.entrySet()) {
                for (KafkaEvent.KafkaEventRecord rec : entry.getValue()) {
                    String topic = resolveTopic(entry.getKey(), rec);
                    JsonNode payload = parsePayload(rec.getValue());

                    if (payload == null || payload.isNull()) {
                        skipped++;
                        continue;
                    }

                    if ("pg1.transaction".equals(topic)) {
                        upsertTransactionMetadata(clientConn, payload);
                        upsertTransactionMetadata(operationalConn, payload);
                        processed++;
                        continue;
                    }

                    if (upsertByTopic(clientConn, operationalConn, topic, payload)) {
                        processed++;
                    } else {
                        Connection targetConn = topic.startsWith("client.") ? clientConn : operationalConn;
                        insertRawEvent(targetConn, topic, rec, payload);
                        processed++;
                    }
                }
            }

            return Map.of("status", "ok", "processed", processed, "skipped", skipped);
        } catch (Exception e) {
            log(context, "ERROR processing records: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return Map.of("status", "error", "message", e.getMessage());
        }
    }

    private boolean upsertByTopic(Connection clientConn, Connection operationalConn, String topic, JsonNode payload) throws Exception {
        return switch (topic) {
            case "client.customers" -> {
                processBusinessEvent(
                    clientConn, topic, String.valueOf(requiredLong(payload, "id")), payload,
                    null,
                    () -> upsertCustomer(clientConn, payload),
                    () -> retryPendingChildren(clientConn, "client", "customers")
                );
                yield true;
            }
            case "client.addresses" -> {
                processBusinessEvent(
                    clientConn, topic,
                    businessKey("customer_id", requiredLong(payload, "customer_id"), "address_type", requiredText(payload, "address_type")),
                    payload,
                    new ChildDependency("fk__client.addresses__client.customers", "client", "addresses", "client", "customers"),
                    () -> upsertAddress(clientConn, payload),
                    null
                );
                yield true;
            }
            case "operational.products" -> {
                processBusinessEvent(
                    operationalConn, topic, requiredText(payload, "name"), payload,
                    null,
                    () -> upsertProduct(operationalConn, payload),
                    () -> retryPendingChildren(operationalConn, "operational", "products")
                );
                yield true;
            }
            case "operational.orders" -> {
                processBusinessEvent(
                    operationalConn, topic, String.valueOf(requiredLong(payload, "id")), payload,
                    null,
                    () -> upsertOrder(operationalConn, payload),
                    () -> retryPendingChildren(operationalConn, "operational", "orders")
                );
                yield true;
            }
            case "operational.order_items" -> {
                processBusinessEvent(
                    operationalConn, topic,
                    businessKey("order_id", requiredLong(payload, "order_id"), "product_name", requiredText(payload, "product_name")),
                    payload,
                    new ChildDependency("fk__operational.order_items__operational.orders", "operational", "order_items", "operational", "orders"),
                    () -> upsertOrderItem(operationalConn, payload),
                    null
                );
                yield true;
            }
            case "operational.contact_numbers" -> {
                processBusinessEvent(
                    operationalConn, topic,
                    businessKey("customer_id", requiredLong(payload, "customer_id"), "phone_type", requiredText(payload, "phone_type")),
                    payload,
                    null,
                    () -> upsertContactNumber(operationalConn, payload),
                    null
                );
                yield true;
            }
            default -> false;
        };
    }

    private void upsertCustomer(Connection conn, JsonNode payload) throws Exception {
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

    private void upsertAddress(Connection conn, JsonNode payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO client.addresses (
                customer_id, address_type, street, city, state, zip_code,
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order,
                source_tx_data_collection_order, idempotency_key
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

    private void upsertProduct(Connection conn, JsonNode payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO operational.products (
                name, category, current_price,
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order,
                source_tx_data_collection_order, idempotency_key
            )
            VALUES (?, ?, ?::numeric, ?, ?, ?, ?, ?, ?)
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
            ps.setString(3, optionalText(payload, "current_price"));
            setSourceMetadata(ps, 4, payload);
            ps.executeUpdate();
        }
    }

    private void upsertOrder(Connection conn, JsonNode payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO operational.orders (
                id, customer_id, order_date, status,
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order,
                source_tx_data_collection_order, idempotency_key
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

    private void upsertOrderItem(Connection conn, JsonNode payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO operational.order_items (
                order_id, product_name, quantity, price_at_purchase,
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order,
                source_tx_data_collection_order, idempotency_key
            )
            VALUES (?, ?, ?, ?::numeric, ?, ?, ?, ?, ?, ?)
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
            ps.setString(4, optionalText(payload, "price_at_purchase"));
            setSourceMetadata(ps, 5, payload);
            ps.executeUpdate();
        }
    }

    private void upsertContactNumber(Connection conn, JsonNode payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO operational.contact_numbers (
                customer_id, phone_type, phone_number,
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order,
                source_tx_data_collection_order, idempotency_key
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

    private void upsertTransactionMetadata(Connection conn, JsonNode payload) throws Exception {
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
            JsonNode dataCollections = payload.get("data_collections");
            ps.setString(4, dataCollections == null || dataCollections.isNull() ? "[]" : dataCollections.toString());
            setNullableLong(ps, 5, optionalLong(payload, "ts_ms"));
            ps.executeUpdate();
        }
    }

    private void insertRawEvent(Connection conn, String topic, KafkaEvent.KafkaEventRecord rec, JsonNode payload) throws Exception {
        String table = topic.startsWith("client.") ? "client.events" : "operational.events";
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO %s (topic, kafka_partition, kafka_offset, payload)
            VALUES (?, ?, ?, ?::jsonb)
            ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
        """.formatted(table))) {
            ps.setString(1, topic);
            ps.setInt(2, rec.getPartition());
            ps.setLong(3, rec.getOffset());
            ps.setString(4, payload.toString());
            ps.executeUpdate();
        }
    }

    private void processBusinessEvent(
        Connection conn,
        String topic,
        String targetBusinessKey,
        JsonNode payload,
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

    private boolean insertProcessedEvent(Connection conn, String topic, String targetBusinessKey, JsonNode payload) throws Exception {
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
            ps.setString(6, payload.toString());
            return ps.executeUpdate() == 1;
        }
    }

    private void postponeForeignKeyEvent(
        Connection conn,
        String topic,
        String targetBusinessKey,
        JsonNode payload,
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
            ps.setString(9, payload.toString());
            ps.setString(10, error.getMessage());
            ps.executeUpdate();
        }
    }

    private void retryPendingChildren(Connection conn, String parentSchema, String parentTable) throws Exception {
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
                    retryPendingChild(
                        conn,
                        rs.getString("event_id"),
                        rs.getString("target_topic"),
                        MAPPER.readTree(rs.getString("payload"))
                    );
                }
            }
        }
    }

    private void retryPendingChild(Connection conn, String eventId, String topic, JsonNode payload) throws Exception {
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

    private void upsertPendingChild(String topic, Connection conn, JsonNode payload) throws Exception {
        switch (topic) {
            case "client.addresses" -> upsertAddress(conn, payload);
            case "operational.order_items" -> upsertOrderItem(conn, payload);
            default -> throw new IllegalArgumentException("Unsupported postponed FK topic: " + topic);
        }
    }

    private void markPendingChildApplied(Connection conn, String eventId) throws Exception {
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

    private void markPendingChildRetry(Connection conn, String eventId, ChildDependency dependency, SQLException error) throws Exception {
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

    private void ensureClientSchema(Connection conn) throws Exception {
        if (!CLIENT_DDL_ENSURED.compareAndSet(false, true)) {
            return;
        }
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
                  PRIMARY KEY (customer_id, address_type),
                  CONSTRAINT "fk__client.addresses__client.customers"
                    FOREIGN KEY (customer_id) REFERENCES client.customers(id)
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

                CREATE TABLE IF NOT EXISTS cdc.processed_events (
                  event_id TEXT PRIMARY KEY,
                  source_tx_id TEXT NOT NULL,
                  source_tx_total_order BIGINT,
                  target_topic TEXT NOT NULL,
                  target_business_key TEXT NOT NULL,
                  processed_at TIMESTAMPTZ DEFAULT NOW(),
                  payload JSONB NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cdc.postponed_fk_events (
                  event_id TEXT PRIMARY KEY,
                  fk_name TEXT NOT NULL,
                  child_schema TEXT NOT NULL,
                  child_table TEXT NOT NULL,
                  parent_schema TEXT NOT NULL,
                  parent_table TEXT NOT NULL,
                  target_topic TEXT NOT NULL,
                  target_business_key TEXT NOT NULL,
                  payload JSONB NOT NULL,
                  error_message TEXT,
                  retry_count INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL DEFAULT 'PENDING',
                  created_at TIMESTAMPTZ DEFAULT NOW(),
                  updated_at TIMESTAMPTZ DEFAULT NOW(),
                  last_retry_at TIMESTAMPTZ
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

                ALTER TABLE client.customers
                  ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);
                ALTER TABLE client.addresses
                  ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);

                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'fk__client.addresses__client.customers'
                  ) THEN
                    ALTER TABLE client.addresses
                      ADD CONSTRAINT "fk__client.addresses__client.customers"
                      FOREIGN KEY (customer_id) REFERENCES client.customers(id) NOT VALID;
                  END IF;
                END $$;
            """);
        }
    }

    private void ensureOperationalSchema(Connection conn) throws Exception {
        if (!OPERATIONAL_DDL_ENSURED.compareAndSet(false, true)) {
            return;
        }
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
                  PRIMARY KEY (order_id, product_name),
                  CONSTRAINT "fk__operational.order_items__operational.orders"
                    FOREIGN KEY (order_id) REFERENCES operational.orders(id),
                  CONSTRAINT "fk__operational.order_items__operational.products"
                    FOREIGN KEY (product_name) REFERENCES operational.products(name)
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

                CREATE TABLE IF NOT EXISTS cdc.processed_events (
                  event_id TEXT PRIMARY KEY,
                  source_tx_id TEXT NOT NULL,
                  source_tx_total_order BIGINT,
                  target_topic TEXT NOT NULL,
                  target_business_key TEXT NOT NULL,
                  processed_at TIMESTAMPTZ DEFAULT NOW(),
                  payload JSONB NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cdc.postponed_fk_events (
                  event_id TEXT PRIMARY KEY,
                  fk_name TEXT NOT NULL,
                  child_schema TEXT NOT NULL,
                  child_table TEXT NOT NULL,
                  parent_schema TEXT NOT NULL,
                  parent_table TEXT NOT NULL,
                  target_topic TEXT NOT NULL,
                  target_business_key TEXT NOT NULL,
                  payload JSONB NOT NULL,
                  error_message TEXT,
                  retry_count INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL DEFAULT 'PENDING',
                  created_at TIMESTAMPTZ DEFAULT NOW(),
                  updated_at TIMESTAMPTZ DEFAULT NOW(),
                  last_retry_at TIMESTAMPTZ
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

                ALTER TABLE operational.products
                  ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);
                ALTER TABLE operational.orders
                  ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);
                ALTER TABLE operational.order_items
                  ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);
                ALTER TABLE operational.contact_numbers
                  ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);

                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'fk__operational.order_items__operational.orders'
                  ) THEN
                    ALTER TABLE operational.order_items
                      ADD CONSTRAINT "fk__operational.order_items__operational.orders"
                      FOREIGN KEY (order_id) REFERENCES operational.orders(id) NOT VALID;
                  END IF;
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'fk__operational.order_items__operational.products'
                  ) THEN
                    ALTER TABLE operational.order_items
                      ADD CONSTRAINT "fk__operational.order_items__operational.products"
                      FOREIGN KEY (product_name) REFERENCES operational.products(name) NOT VALID;
                  END IF;
                END $$;
            """);
        }
    }

    private Connection openConnection(String host, String port, String dbName, String user, String secretArn) throws Exception {
        String password = getDbPasswordFromSecretsManager(secretArn);
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
        return DriverManager.getConnection(url, user, password);
    }

    private String getDbPasswordFromSecretsManager(String secretArn) {
        synchronized (PW_LOCK) {
            String cached = CACHED_PASSWORDS.get(secretArn);
            if (cached != null && !cached.isBlank()) {
                return cached;
            }

            String regionName = env("AWS_REGION", env("AWS_DEFAULT_REGION", "us-east-1"));
            try (SecretsManagerClient sm = SecretsManagerClient.builder().region(Region.of(regionName)).build()) {
                GetSecretValueResponse resp = sm.getSecretValue(GetSecretValueRequest.builder()
                    .secretId(secretArn)
                    .build());

                JsonNode root = MAPPER.readTree(resp.secretString());
                JsonNode pw = root.get("password");
                if (pw == null || pw.asText().isBlank()) {
                    throw new IllegalStateException("Missing password in secret JSON for " + secretArn);
                }

                String value = pw.asText();
                CACHED_PASSWORDS.put(secretArn, value);
                return value;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read DB password for " + secretArn + ": " + e.getMessage(), e);
            }
        }
    }

    private static JsonNode parsePayload(String value) throws Exception {
        if (value == null || value.isBlank()) {
            return null;
        }
        String decoded = decodePossiblyBase64(value);
        return MAPPER.readTree(decoded);
    }

    private static String resolveTopic(String recordKey, KafkaEvent.KafkaEventRecord rec) {
        if (rec.getTopic() != null && !rec.getTopic().isBlank()) {
            return rec.getTopic();
        }
        if (recordKey == null) {
            return "unknown";
        }
        int dash = recordKey.lastIndexOf('-');
        return dash > 0 ? recordKey.substring(0, dash) : recordKey;
    }

    static String requiredText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || value.asText().isBlank()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return value.asText();
    }

    private static String optionalText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static Long optionalLong(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isNumber()) {
            return value.asLong();
        }
        String asText = value.asText();
        return asText == null || asText.isBlank() ? null : Long.parseLong(asText);
    }

    private static long requiredLong(JsonNode node, String field) {
        Long value = optionalLong(node, field);
        if (value == null) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return value;
    }

    private static int requiredInt(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return value.asInt();
    }

    private static boolean isForeignKeyViolation(SQLException error) {
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

    private static ChildDependency fallbackDependency(String topic) {
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

    private static String foreignKeyName(SQLException error) {
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

    private static String foreignKeyName(String message) {
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

    static String idempotencyKey(JsonNode payload, String targetTopic, String targetBusinessKey) {
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

    static String processedSourceTxId(JsonNode payload) {
        String sourceTxId = optionalText(payload, "source_tx_id");
        return sourceTxId == null || sourceTxId.isBlank() ? "no-tx" : sourceTxId;
    }

    static String businessKey(String firstName, Object firstValue, String secondName, Object secondValue) {
        return "%s=%s|%s=%s".formatted(firstName, firstValue, secondName, secondValue);
    }

    private static void setSourceMetadata(PreparedStatement ps, int startIndex, JsonNode payload) throws SQLException {
        ps.setString(startIndex, optionalText(payload, "source_record_type"));
        setNullableLong(ps, startIndex + 1, optionalLong(payload, "source_ts_ms"));
        ps.setString(startIndex + 2, optionalText(payload, "source_tx_id"));
        setNullableLong(ps, startIndex + 3, optionalLong(payload, "source_tx_total_order"));
        setNullableLong(ps, startIndex + 4, optionalLong(payload, "source_tx_data_collection_order"));
        ps.setString(startIndex + 5, optionalText(payload, "idempotency_key"));
    }

    private static void setNullableLong(PreparedStatement ps, int idx, Long value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.BIGINT);
        } else {
            ps.setLong(idx, value);
        }
    }

    private static String decodePossiblyBase64(String s) {
        try {
            byte[] decoded = Base64.getDecoder().decode(s);
            String asText = new String(decoded, StandardCharsets.UTF_8);
            return looksReasonableText(asText) ? asText : s;
        } catch (IllegalArgumentException ex) {
            return s;
        }
    }

    private static boolean looksReasonableText(String text) {
        if (text == null) {
            return false;
        }
        if (text.isEmpty()) {
            return true;
        }
        int printable = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\n' || c == '\r' || c == '\t' || (c >= 32 && c < 127)) {
                printable++;
            }
        }
        return (printable * 1.0 / text.length()) > 0.85;
    }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v;
    }

    private static String mustEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required env var: " + name);
        }
        return v;
    }

    private static void log(Context context, String message) {
        if (context != null && context.getLogger() != null) {
            context.getLogger().log(message + "\n");
        }
    }
}
