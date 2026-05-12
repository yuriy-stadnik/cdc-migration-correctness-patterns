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

    // --- env ---
    private final String dbHost     = mustEnv("DB_HOST");
    private final String dbPort     = env("DB_PORT", "5432");
    private final String dbName     = mustEnv("DB_NAME");
    private final String dbUser     = mustEnv("DB_USER");
    private final String secretArn  = mustEnv("DB_SECRET_ARN");
    private final String tableName  = env("TABLE_NAME", "events");

    // --- SQL ---
    private final String ddl = """
        CREATE TABLE IF NOT EXISTS %s (
          id          BIGSERIAL PRIMARY KEY,
          topic       TEXT NOT NULL,
          partition   INTEGER NOT NULL,
          kafka_offset      BIGINT NOT NULL,
          event_ts    TIMESTAMPTZ DEFAULT NOW(),
          payload     JSONB NOT NULL,
          UNIQUE(topic, partition, offset)
        );
        """.formatted(quoteIdent(tableName));

    private final String insertSql = """
        INSERT INTO %s (topic, partition, offset, payload)
        VALUES (?, ?, ?, ?::jsonb)
        ON CONFLICT (topic, partition, kafka_offset) DO NOTHING;
        """.formatted(quoteIdent(tableName));

    // Cache secret between invocations within the same Lambda execution environment
    private static volatile String cachedPassword = null;
    private static final Object PW_LOCK = new Object();
    private static final AtomicBoolean ddlEnsured = new AtomicBoolean(false);

    @Override
    public Map<String, Object> handleRequest(KafkaEvent event, Context context) {
        Map<String, List<KafkaEvent.KafkaEventRecord>> records = event.getRecords();
        context.getLogger().log("Records keys: " + (records == null ? "null" : records.keySet()) + "\n");
        if (records == null || records.isEmpty()) {
            return Map.of("status", "ok", "processed", 0);
        }

        // Open one connection per invocation (safe for lab; consider RDS Proxy for real load)
        try (Connection conn = openConnection()) {

            // Ensure table exists once per warm Lambda container
            if (ddlEnsured.compareAndSet(false, true)) {
                try (Statement st = conn.createStatement()) {
                    st.execute(ddl);
                }
            }

            int processed = 0;

            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                for (Map.Entry<String, List<KafkaEvent.KafkaEventRecord>> entry : records.entrySet()) {
                    for (KafkaEvent.KafkaEventRecord rec : entry.getValue()) {

                        String topic = (rec.getTopic() == null || rec.getTopic().isBlank())
                                ? parseTopicFromKey(entry.getKey())
                                : rec.getTopic();

                        int partition = rec.getPartition();
                        long kafka_offset = rec.getOffset();

                        String valueStr = rec.getValue() == null ? "" : rec.getValue();
                        String decoded = decodePossiblyBase64(valueStr);

                        JsonNode payloadNode;
                        try {
                            payloadNode = MAPPER.readTree(decoded);
                        } catch (Exception e) {
                            payloadNode = MAPPER.createObjectNode().put("raw", decoded);
                        }

                        String payloadJson = payloadNode.toString();

                        ps.setString(1, topic);
                        ps.setInt(2, partition);
                        ps.setLong(3, kafka_offset);
                        ps.setString(4, payloadJson);

                        ps.executeUpdate();
                        processed++;
                    }
                }
            }

            return Map.of("status", "ok", "processed", processed);

        } catch (Exception e) {
            // Don’t leak secrets; log minimal info
            if (context != null && context.getLogger() != null) {
                context.getLogger().log("ERROR writing to Aurora: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            return Map.of("status", "error", "message", e.getMessage());
        }
    }

    private Connection openConnection() throws Exception {
        String password = getDbPasswordFromSecretsManager();

        // Example: jdbc:postgresql://host:5432/db
        String url = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName;

        // For lab: keep simple. If you enforce SSL, add ?sslmode=require
        // url += "?sslmode=require";

        // Make sure you included the PostgreSQL JDBC driver in your shaded/fat jar.
        return DriverManager.getConnection(url, dbUser, password);
    }

    private String getDbPasswordFromSecretsManager() {
        if (cachedPassword != null && !cachedPassword.isBlank()) return cachedPassword;

        synchronized (PW_LOCK) {
            if (cachedPassword != null && !cachedPassword.isBlank()) return cachedPassword;

            // Use Lambda region automatically if possible; AWS_REGION is typically set.
            String regionName = env("AWS_REGION", env("AWS_DEFAULT_REGION", "us-east-1"));

            try (SecretsManagerClient sm = SecretsManagerClient.builder()
                    .region(Region.of(regionName))
                    .build()) {

                GetSecretValueResponse resp = sm.getSecretValue(GetSecretValueRequest.builder()
                        .secretId(secretArn)
                        .build());

                String secretString = resp.secretString();
                if (secretString == null || secretString.isBlank()) {
                    throw new IllegalStateException("SecretString is empty for DB secret");
                }

                // RDS managed secret JSON looks like:
                // {"username":"...","password":"...","engine":"postgres","host":"...","port":5432,"dbname":"..."}
                JsonNode root = MAPPER.readTree(secretString);
                JsonNode pw = root.get("password");
                if (pw == null || pw.asText().isBlank()) {
                    throw new IllegalStateException("Missing 'password' in DB secret JSON");
                }

                cachedPassword = pw.asText();
                return cachedPassword;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read DB password from Secrets Manager: " + e.getMessage(), e);
            }
        }
    }

    // --- helpers (same as before, minor cleanup) ---

    private static String decodePossiblyBase64(String s) {
        if (s == null) return "";
        try {
            byte[] decoded = Base64.getDecoder().decode(s);
            String asText = new String(decoded, StandardCharsets.UTF_8);
            if (looksReasonableText(asText)) return asText;
            return s;
        } catch (IllegalArgumentException ex) {
            return s;
        }
    }

    private static boolean looksReasonableText(String text) {
        if (text == null) return false;
        int len = text.length();
        if (len == 0) return true;
        int printable = 0;
        for (int i = 0; i < len; i++) {
            char c = text.charAt(i);
            if (c == '\n' || c == '\r' || c == '\t' || (c >= 32 && c < 127)) printable++;
        }
        return (printable * 1.0 / len) > 0.85;
    }

    private static String parseTopicFromKey(String recordKey) {
        if (recordKey == null) return "unknown";
        int dash = recordKey.lastIndexOf('-');
        if (dash > 0) return recordKey.substring(0, dash);
        return recordKey;
    }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v;
    }

    private static String mustEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required env var: " + name);
        }
        return v;
    }

    // Very small identifier quoting to avoid breaking if tableName is "events"
    // For lab only; do not allow untrusted user input for identifiers.
    private static String quoteIdent(String ident) {
        if (ident == null || ident.isBlank()) return "events";
        // allow simple identifiers, otherwise quote
        if (ident.matches("[a-zA-Z_][a-zA-Z0-9_]*")) return ident;
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }
}