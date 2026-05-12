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
import java.sql.Statement;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Handler implements RequestHandler<KafkaEvent, Map<String, Object>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Object PW_LOCK = new Object();
    private static final AtomicBoolean ddlEnsured = new AtomicBoolean(false);
    private static volatile String cachedPassword = null;

    private final String dbHost = mustEnv("DB_HOST");
    private final String dbPort = env("DB_PORT", "5432");
    private final String dbName = mustEnv("DB_NAME");
    private final String dbUser = mustEnv("DB_USER");
    private final String secretArn = mustEnv("DB_SECRET_ARN");

    @Override
    public Map<String, Object> handleRequest(KafkaEvent event, Context context) {
        Map<String, List<KafkaEvent.KafkaEventRecord>> records = event.getRecords();
        log(context, "Records keys: " + (records == null ? "null" : records.keySet()));

        if (records == null || records.isEmpty()) {
            return Map.of("status", "ok", "processed", 0);
        }

        try (Connection conn = openConnection()) {
            ensureSchema(conn);

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

                    if (upsertByTopic(conn, topic, payload)) {
                        processed++;
                    } else {
                        insertRawEvent(conn, topic, rec, payload);
                        processed++;
                    }
                }
            }

            return Map.of("status", "ok", "processed", processed, "skipped", skipped);
        } catch (Exception e) {
            log(context, "ERROR writing to Aurora: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return Map.of("status", "error", "message", e.getMessage());
        }
    }

    private boolean upsertByTopic(Connection conn, String topic, JsonNode payload) throws Exception {
        return switch (topic) {
            case "operational.products" -> {
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO operational.products (name, category, current_price)
                    VALUES (?, ?, ?::numeric)
                    ON CONFLICT (name) DO UPDATE
                    SET category = EXCLUDED.category,
                        current_price = EXCLUDED.current_price;
                    """)) {
                    ps.setString(1, requiredText(payload, "name"));
                    ps.setString(2, optionalText(payload, "category"));
                    ps.setString(3, optionalText(payload, "current_price"));
                    ps.executeUpdate();
                }
                yield true;
            }
            case "operational.orders" -> {
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO operational.orders (id, customer_id, order_date, status)
                    VALUES (?, ?, ?::timestamptz, ?)
                    ON CONFLICT (id) DO UPDATE
                    SET customer_id = EXCLUDED.customer_id,
                        order_date = EXCLUDED.order_date,
                        status = EXCLUDED.status;
                    """)) {
                    ps.setLong(1, requiredLong(payload, "id"));
                    ps.setLong(2, requiredLong(payload, "customer_id"));
                    ps.setString(3, requiredText(payload, "order_date"));
                    ps.setString(4, optionalText(payload, "status"));
                    ps.executeUpdate();
                }
                yield true;
            }
            case "operational.order_items" -> {
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO operational.order_items (order_id, product_name, quantity, price_at_purchase)
                    VALUES (?, ?, ?, ?::numeric)
                    ON CONFLICT (order_id, product_name) DO UPDATE
                    SET quantity = EXCLUDED.quantity,
                        price_at_purchase = EXCLUDED.price_at_purchase;
                    """)) {
                    ps.setLong(1, requiredLong(payload, "order_id"));
                    ps.setString(2, requiredText(payload, "product_name"));
                    ps.setInt(3, requiredInt(payload, "quantity"));
                    ps.setString(4, optionalText(payload, "price_at_purchase"));
                    ps.executeUpdate();
                }
                yield true;
            }
            case "operational.addresses" -> {
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO operational.addresses (customer_id, address_type, street, city, state, zip_code)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (customer_id, address_type) DO UPDATE
                    SET street = EXCLUDED.street,
                        city = EXCLUDED.city,
                        state = EXCLUDED.state,
                        zip_code = EXCLUDED.zip_code;
                    """)) {
                    ps.setLong(1, requiredLong(payload, "customer_id"));
                    ps.setString(2, requiredText(payload, "address_type"));
                    ps.setString(3, optionalText(payload, "street"));
                    ps.setString(4, optionalText(payload, "city"));
                    ps.setString(5, optionalText(payload, "state"));
                    ps.setString(6, optionalText(payload, "zip_code"));
                    ps.executeUpdate();
                }
                yield true;
            }
            case "operational.contact_numbers" -> {
                try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO operational.contact_numbers (customer_id, phone_type, phone_number)
                    VALUES (?, ?, ?)
                    ON CONFLICT (customer_id, phone_type) DO UPDATE
                    SET phone_number = EXCLUDED.phone_number;
                    """)) {
                    ps.setLong(1, requiredLong(payload, "customer_id"));
                    ps.setString(2, requiredText(payload, "phone_type"));
                    ps.setString(3, requiredText(payload, "phone_number"));
                    ps.executeUpdate();
                }
                yield true;
            }
            default -> false;
        };
    }

    private void ensureSchema(Connection conn) throws Exception {
        if (!ddlEnsured.compareAndSet(false, true)) {
            return;
        }

        try (Statement st = conn.createStatement()) {
            st.execute("""
                CREATE SCHEMA IF NOT EXISTS operational;

                CREATE TABLE IF NOT EXISTS operational.products (
                  name TEXT PRIMARY KEY,
                  category TEXT,
                  current_price DECIMAL(10,2)
                );

                CREATE TABLE IF NOT EXISTS operational.orders (
                  id BIGINT PRIMARY KEY,
                  customer_id BIGINT,
                  order_date TIMESTAMPTZ,
                  status TEXT
                );

                CREATE TABLE IF NOT EXISTS operational.order_items (
                  order_id BIGINT NOT NULL,
                  product_name TEXT NOT NULL,
                  quantity INTEGER,
                  price_at_purchase DECIMAL(10,2),
                  PRIMARY KEY (order_id, product_name)
                );

                CREATE TABLE IF NOT EXISTS operational.addresses (
                  customer_id BIGINT NOT NULL,
                  address_type TEXT NOT NULL,
                  street TEXT,
                  city TEXT,
                  state TEXT,
                  zip_code TEXT,
                  PRIMARY KEY (customer_id, address_type)
                );

                CREATE TABLE IF NOT EXISTS operational.contact_numbers (
                  customer_id BIGINT NOT NULL,
                  phone_type TEXT NOT NULL,
                  phone_number TEXT NOT NULL,
                  PRIMARY KEY (customer_id, phone_type)
                );

                CREATE TABLE IF NOT EXISTS events (
                  id BIGSERIAL PRIMARY KEY,
                  topic TEXT NOT NULL,
                  kafka_partition INTEGER NOT NULL,
                  kafka_offset BIGINT NOT NULL,
                  event_ts TIMESTAMPTZ DEFAULT NOW(),
                  payload JSONB NOT NULL,
                  UNIQUE(topic, kafka_partition, kafka_offset)
                );
                """);
        }
    }

    private void insertRawEvent(Connection conn, String topic, KafkaEvent.KafkaEventRecord rec, JsonNode payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
            INSERT INTO events (topic, kafka_partition, kafka_offset, payload)
            VALUES (?, ?, ?, ?::jsonb)
            ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING;
            """)) {
            ps.setString(1, topic);
            ps.setInt(2, rec.getPartition());
            ps.setLong(3, rec.getOffset());
            ps.setString(4, payload.toString());
            ps.executeUpdate();
        }
    }

    private Connection openConnection() throws Exception {
        String password = getDbPasswordFromSecretsManager();
        String url = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName;
        return DriverManager.getConnection(url, dbUser, password);
    }

    private String getDbPasswordFromSecretsManager() {
        if (cachedPassword != null && !cachedPassword.isBlank()) {
            return cachedPassword;
        }

        synchronized (PW_LOCK) {
            if (cachedPassword != null && !cachedPassword.isBlank()) {
                return cachedPassword;
            }

            String regionName = env("AWS_REGION", env("AWS_DEFAULT_REGION", "us-east-1"));
            try (SecretsManagerClient sm = SecretsManagerClient.builder().region(Region.of(regionName)).build()) {
                GetSecretValueResponse resp = sm.getSecretValue(GetSecretValueRequest.builder()
                    .secretId(secretArn)
                    .build());

                JsonNode root = MAPPER.readTree(resp.secretString());
                JsonNode pw = root.get("password");
                if (pw == null || pw.asText().isBlank()) {
                    throw new IllegalStateException("Missing password in DB secret JSON");
                }

                cachedPassword = pw.asText();
                return cachedPassword;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read DB password from Secrets Manager: " + e.getMessage(), e);
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

    private static long requiredLong(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return value.asLong();
    }

    private static int requiredInt(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return value.asInt();
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
