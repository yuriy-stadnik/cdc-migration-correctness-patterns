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
import java.sql.SQLException;
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

                    Connection targetConn = topic.startsWith("client.") ? clientConn : operationalConn;
                    if (upsertByTopic(targetConn, topic, payload)) {
                        processed++;
                    } else {
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

    private boolean upsertByTopic(Connection conn, String topic, JsonNode payload) throws Exception {
        return switch (topic) {
            case "client.customers" -> {
                upsertCustomer(conn, payload);
                yield true;
            }
            case "client.addresses" -> {
                upsertAddress(conn, payload);
                yield true;
            }
            case "operational.products" -> {
                upsertProduct(conn, payload);
                yield true;
            }
            case "operational.orders" -> {
                upsertOrder(conn, payload);
                yield true;
            }
            case "operational.order_items" -> {
                upsertOrderItem(conn, payload);
                yield true;
            }
            case "operational.contact_numbers" -> {
                upsertContactNumber(conn, payload);
                yield true;
            }
            default -> false;
        };
    }

    private void upsertCustomer(Connection conn, JsonNode payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO client.customers (
                id, first_name, last_name, email, status, created_at, updated_at,
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order, source_tx_data_collection_order
            )
            VALUES (?, ?, ?, ?, ?, ?::timestamptz, ?::timestamptz, ?, ?, ?, ?, ?)
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
                source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order
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
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order, source_tx_data_collection_order
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (customer_id, address_type) DO UPDATE
            SET street = EXCLUDED.street,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip_code = EXCLUDED.zip_code,
                source_record_type = EXCLUDED.source_record_type,
                source_ts_ms = EXCLUDED.source_ts_ms,
                source_tx_id = EXCLUDED.source_tx_id,
                source_tx_total_order = EXCLUDED.source_tx_total_order,
                source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order
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
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order, source_tx_data_collection_order
            )
            VALUES (?, ?, ?::numeric, ?, ?, ?, ?, ?)
            ON CONFLICT (name) DO UPDATE
            SET category = EXCLUDED.category,
                current_price = EXCLUDED.current_price,
                source_record_type = EXCLUDED.source_record_type,
                source_ts_ms = EXCLUDED.source_ts_ms,
                source_tx_id = EXCLUDED.source_tx_id,
                source_tx_total_order = EXCLUDED.source_tx_total_order,
                source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order
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
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order, source_tx_data_collection_order
            )
            VALUES (?, ?, ?::timestamptz, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE
            SET customer_id = EXCLUDED.customer_id,
                order_date = EXCLUDED.order_date,
                status = EXCLUDED.status,
                source_record_type = EXCLUDED.source_record_type,
                source_ts_ms = EXCLUDED.source_ts_ms,
                source_tx_id = EXCLUDED.source_tx_id,
                source_tx_total_order = EXCLUDED.source_tx_total_order,
                source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order
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
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order, source_tx_data_collection_order
            )
            VALUES (?, ?, ?, ?::numeric, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id, product_name) DO UPDATE
            SET quantity = EXCLUDED.quantity,
                price_at_purchase = EXCLUDED.price_at_purchase,
                source_record_type = EXCLUDED.source_record_type,
                source_ts_ms = EXCLUDED.source_ts_ms,
                source_tx_id = EXCLUDED.source_tx_id,
                source_tx_total_order = EXCLUDED.source_tx_total_order,
                source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order
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
                source_record_type, source_ts_ms, source_tx_id, source_tx_total_order, source_tx_data_collection_order
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (customer_id, phone_type) DO UPDATE
            SET phone_number = EXCLUDED.phone_number,
                source_record_type = EXCLUDED.source_record_type,
                source_ts_ms = EXCLUDED.source_ts_ms,
                source_tx_id = EXCLUDED.source_tx_id,
                source_tx_total_order = EXCLUDED.source_tx_total_order,
                source_tx_data_collection_order = EXCLUDED.source_tx_data_collection_order
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

    private static String requiredText(JsonNode node, String field) {
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

    private static void setSourceMetadata(PreparedStatement ps, int startIndex, JsonNode payload) throws SQLException {
        ps.setString(startIndex, optionalText(payload, "source_record_type"));
        setNullableLong(ps, startIndex + 1, optionalLong(payload, "source_ts_ms"));
        ps.setString(startIndex + 2, optionalText(payload, "source_tx_id"));
        setNullableLong(ps, startIndex + 3, optionalLong(payload, "source_tx_total_order"));
        setNullableLong(ps, startIndex + 4, optionalLong(payload, "source_tx_data_collection_order"));
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
