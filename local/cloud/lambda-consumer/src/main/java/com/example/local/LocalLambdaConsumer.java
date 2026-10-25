package com.example.local;

import com.example.cdc.apply.ConnectionProvider;
import com.example.cdc.apply.SchemaMigrator;
import com.example.cdc.apply.TransactionApplyService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Local long-running equivalent of the Lambda ingestion/apply entry point. */
public final class LocalLambdaConsumer {
    public static void main(String[] args) throws Exception { new LocalLambdaConsumer().run(); }

    private void run() throws Exception {
        List<String> topics = java.util.Arrays.stream(env("KAFKA_TOPICS", "cdc.tx.control.v2,cdc.tx.mutations.v2,cdc.tx.dlq.v2").split(","))
                .map(String::trim).filter(s -> !s.isBlank()).toList();
        String jdbc = "jdbc:postgresql://%s:%s/%s".formatted(requiredEnv("DESTINATION_DB_HOST"),
                env("DESTINATION_DB_PORT", "5432"), requiredEnv("DESTINATION_DB_NAME"));
        String user = requiredEnv("DESTINATION_DB_USER"), password = requiredEnv("DESTINATION_DB_PASSWORD");
        ConnectionProvider connections = () -> DriverManager.getConnection(jdbc, user, password);
        waitForDatabase(connections);
        SchemaMigrator.migrate(connections);
        TransactionApplyService service = new TransactionApplyService(connections, env("WORKER_ID", "local-worker"));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(topics);
            System.out.println("transaction consumer subscribed to " + topics);
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    if (record.value() == null || record.value().isBlank()) continue;
                    service.stage(record.topic(), record.partition(), record.offset(), record.value());
                    consumer.commitSync(safeOffset(record.topic(), record.partition(), record.offset()));
                }
                int applied = service.drain(100);
                if (!records.isEmpty() || applied > 0) System.out.printf("staged=%d applied=%d%n", records.count(), applied);
            }
        }
    }

    static Map<TopicPartition, OffsetAndMetadata> safeOffset(String topic, int partition, long durableOffset) {
        return Map.of(new TopicPartition(topic, partition), new OffsetAndMetadata(durableOffset + 1));
    }
    private Properties consumerProperties() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "cloud-kafka:39092"));
        p.put(ConsumerConfig.GROUP_ID_CONFIG, env("KAFKA_GROUP_ID", "transaction-apply-v2"));
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return p;
    }
    private static void waitForDatabase(ConnectionProvider connections) throws InterruptedException {
        while (true) {
            try (var ignored = connections.open()) { return; }
            catch (Exception unavailable) { System.out.println("waiting for destination DB: " + unavailable.getMessage()); Thread.sleep(3000); }
        }
    }
    private static String requiredEnv(String name){String value=System.getenv(name);if(value==null||value.isBlank())throw new IllegalStateException("Missing required environment variable: "+name);return value;}
    private static String env(String name,String fallback){String value=System.getenv(name);return value==null||value.isBlank()?fallback:value;}
}
