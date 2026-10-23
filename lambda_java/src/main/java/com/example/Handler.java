package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.example.cdc.apply.ConnectionProvider;
import com.example.cdc.apply.SchemaMigrator;
import com.example.cdc.apply.TransactionApplyService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/** MSK entry point: durably stage every delivery, then opportunistically drain READY transactions. */
public final class Handler implements RequestHandler<KafkaEvent, Map<String, Object>> {
    private volatile TransactionApplyService service;

    @Override
    public Map<String, Object> handleRequest(KafkaEvent event, Context context) {
        try {
            TransactionApplyService apply = service();
            int staged = 0;
            Map<String, List<KafkaEvent.KafkaEventRecord>> records = event == null ? null : event.getRecords();
            if (records != null) {
                for (Map.Entry<String, List<KafkaEvent.KafkaEventRecord>> entry : records.entrySet()) {
                    for (KafkaEvent.KafkaEventRecord record : entry.getValue()) {
                        if (record.getValue() == null) continue;
                        apply.stage(resolveTopic(entry.getKey(), record), record.getPartition(), record.getOffset(),
                                decodeValue(record.getValue()));
                        staged++;
                    }
                }
            }
            int applied = apply.drain(Integer.parseInt(env("MAX_TRANSACTIONS_PER_INVOCATION", "20")));
            return Map.of("status", "ok", "staged", staged, "applied", applied);
        } catch (Exception failure) {
            if (context != null) context.getLogger().log("transaction ingestion/apply failed: " + failure + "\n");
            // Lambda MSK advances offsets only when invocation succeeds. Never turn failure into a success payload.
            throw new RuntimeException("transaction ingestion/apply failed", failure);
        }
    }

    private TransactionApplyService service() throws Exception {
        if (service != null) return service;
        synchronized (this) {
            if (service != null) return service;
            String jdbc = "jdbc:postgresql://%s:%s/%s".formatted(requiredEnv("DESTINATION_DB_HOST"),
                    env("DESTINATION_DB_PORT", "5432"), requiredEnv("DESTINATION_DB_NAME"));
            String user = requiredEnv("DESTINATION_DB_USER");
            String password = loadPassword(requiredEnv("DESTINATION_DB_SECRET_ARN"));
            ConnectionProvider connections = () -> DriverManager.getConnection(jdbc, user, password);
            SchemaMigrator.migrate(connections);
            service = new TransactionApplyService(connections, env("AWS_LAMBDA_LOG_STREAM_NAME", "lambda-worker"));
            return service;
        }
    }

    static String decodeValue(String encoded) {
        return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    static String resolveTopic(String mapKey, KafkaEvent.KafkaEventRecord record) {
        if (record.getTopic() != null && !record.getTopic().isBlank()) return record.getTopic();
        int delimiter = mapKey == null ? -1 : mapKey.lastIndexOf('-');
        return delimiter > 0 ? mapKey.substring(0, delimiter) : mapKey;
    }

    private static String loadPassword(String secretArn) throws Exception {
        try (SecretsManagerClient client = SecretsManagerClient.builder()
                .region(Region.of(env("AWS_REGION", "us-east-1"))).build()) {
            String secret = client.getSecretValue(GetSecretValueRequest.builder().secretId(secretArn).build()).secretString();
            return com.example.cdc.protocol.ProtocolV2Codec.mapper().readTree(secret).path("password").asText();
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalStateException("Missing required environment variable: " + name);
        return value;
    }
    private static String env(String name, String fallback) {
        String value = System.getenv(name); return value == null || value.isBlank() ? fallback : value;
    }
}
