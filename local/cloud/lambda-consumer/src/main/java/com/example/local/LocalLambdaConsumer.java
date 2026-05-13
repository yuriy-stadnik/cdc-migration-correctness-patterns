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
    private final String jdbcUrl = "jdbc:postgresql://%s:%s/%s".formatted(
            requiredEnv("DB_HOST"),
            env("DB_PORT", "5432"),
            requiredEnv("DB_NAME")
    );
    private final String dbUser = requiredEnv("DB_USER");
    private final String dbPassword = requiredEnv("DB_PASSWORD");

    public static void main(String[] args) throws Exception {
        new LocalLambdaConsumer().run();
    }

    private void run() throws Exception {
        System.out.printf("Starting local Lambda replacement for topics: %s%n", String.join(", ", topics));

        try (Connection conn = waitForDb(); KafkaConsumer<String, String> consumer = waitForConsumer()) {
            ensureSchema(conn);
            consumer.subscribe(topics);

            while (true) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                    if (record.value() == null || record.value().isBlank()) {
                        continue;
                    }

                    Map<String, Object> payload = MAPPER.readValue(record.value(), MAP_TYPE);
                    try {
                        upsert(conn, record.topic(), record.partition(), record.offset(), payload);
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

    private Connection waitForDb() throws InterruptedException {
        while (true) {
            try {
                Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
                conn.setAutoCommit(true);
                return conn;
            } catch (Exception e) {
                System.out.printf("Waiting for cloud-postgres: %s%n", e.getMessage());
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

    private void ensureSchema(Connection conn) throws Exception {
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

    private void upsert(Connection conn, String topic, int partition, long offset, Map<String, Object> payload) throws Exception {
        switch (topic) {
            case "operational.products" -> upsertProduct(conn, payload);
            case "operational.orders" -> upsertOrder(conn, payload);
            case "operational.order_items" -> upsertOrderItem(conn, payload);
            case "operational.addresses" -> upsertAddress(conn, payload);
            case "operational.contact_numbers" -> upsertContactNumber(conn, payload);
            default -> insertRawEvent(conn, topic, partition, offset, payload);
        }
    }

    private void upsertProduct(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO operational.products (name, category, current_price)
                VALUES (?, ?, ?)
                ON CONFLICT (name) DO UPDATE
                SET category = EXCLUDED.category,
                    current_price = EXCLUDED.current_price
                """)) {
            ps.setString(1, requiredText(payload, "name"));
            ps.setString(2, optionalText(payload, "category"));
            ps.setBigDecimal(3, optionalDecimal(payload, "current_price"));
            ps.executeUpdate();
        }
    }

    private void upsertOrder(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO operational.orders (id, customer_id, order_date, status)
                VALUES (?, ?, ?::timestamptz, ?)
                ON CONFLICT (id) DO UPDATE
                SET customer_id = EXCLUDED.customer_id,
                    order_date = EXCLUDED.order_date,
                    status = EXCLUDED.status
                """)) {
            ps.setLong(1, requiredLong(payload, "id"));
            ps.setLong(2, requiredLong(payload, "customer_id"));
            ps.setString(3, optionalText(payload, "order_date"));
            ps.setString(4, optionalText(payload, "status"));
            ps.executeUpdate();
        }
    }

    private void upsertOrderItem(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO operational.order_items (order_id, product_name, quantity, price_at_purchase)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (order_id, product_name) DO UPDATE
                SET quantity = EXCLUDED.quantity,
                    price_at_purchase = EXCLUDED.price_at_purchase
                """)) {
            ps.setLong(1, requiredLong(payload, "order_id"));
            ps.setString(2, requiredText(payload, "product_name"));
            ps.setInt(3, requiredInt(payload, "quantity"));
            ps.setBigDecimal(4, optionalDecimal(payload, "price_at_purchase"));
            ps.executeUpdate();
        }
    }

    private void upsertAddress(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO operational.addresses (customer_id, address_type, street, city, state, zip_code)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (customer_id, address_type) DO UPDATE
                SET street = EXCLUDED.street,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code
                """)) {
            ps.setLong(1, requiredLong(payload, "customer_id"));
            ps.setString(2, requiredText(payload, "address_type"));
            ps.setString(3, optionalText(payload, "street"));
            ps.setString(4, optionalText(payload, "city"));
            ps.setString(5, optionalText(payload, "state"));
            ps.setString(6, optionalText(payload, "zip_code"));
            ps.executeUpdate();
        }
    }

    private void upsertContactNumber(Connection conn, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO operational.contact_numbers (customer_id, phone_type, phone_number)
                VALUES (?, ?, ?)
                ON CONFLICT (customer_id, phone_type) DO UPDATE
                SET phone_number = EXCLUDED.phone_number
                """)) {
            ps.setLong(1, requiredLong(payload, "customer_id"));
            ps.setString(2, requiredText(payload, "phone_type"));
            ps.setString(3, requiredText(payload, "phone_number"));
            ps.executeUpdate();
        }
    }

    private void insertRawEvent(Connection conn, String topic, int partition, long offset, Map<String, Object> payload) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO events (topic, kafka_partition, kafka_offset, payload)
                VALUES (?, ?, ?, ?::jsonb)
                ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
                """)) {
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

    private static String requiredText(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return value.toString();
    }

    private static String optionalText(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        return value == null ? null : value.toString();
    }

    private static long requiredLong(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(requiredText(payload, field));
    }

    private static int requiredInt(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(requiredText(payload, field));
    }

    private static BigDecimal optionalDecimal(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        return value == null ? null : new BigDecimal(value.toString());
    }
}
