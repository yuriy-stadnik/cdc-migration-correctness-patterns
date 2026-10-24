package com.example.cdc.projector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

public final class TransactionProjectorJob {
    private TransactionProjectorJob() {}

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "source-kafka:29092");
        String captureTopic = env("CAPTURE_TOPIC", com.example.cdc.protocol.ProtocolV2.CAPTURE_TOPIC);
        if (!captureTopic.equals(com.example.cdc.protocol.ProtocolV2.CAPTURE_TOPIC))
            throw new IllegalArgumentException("capture topic mismatch");
        String databaseId = env("SOURCE_DATABASE_ID", "legacy-postgres");
        String incarnation = env("SOURCE_INCARNATION", "local-1");
        String scope = env("CAPTURE_SCOPE_ID", com.example.cdc.protocol.ProtocolV2.GENERATION);

        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();
        execution.setParallelism(1);
        execution.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        execution.getCheckpointConfig().setMinPauseBetweenCheckpoints(2_000);
        execution.getCheckpointConfig().setCheckpointTimeout(120_000);

        KafkaSource<CaptureRecord> source = KafkaSource.<CaptureRecord>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(java.util.List.of(captureTopic))
                .setGroupId("transaction-projector-v2-" + incarnation)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("isolation.level", "read_committed")
                .setDeserializer(new CaptureDeserializer())
                .build();

        SerializationSchema<OutputRecord> keySerializer = element -> element.key().getBytes(StandardCharsets.UTF_8);
        SerializationSchema<OutputRecord> valueSerializer = element -> element.value().getBytes(StandardCharsets.UTF_8);
        KafkaSink<OutputRecord> sink = KafkaSink.<OutputRecord>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("transaction-projector-v2-" + incarnation + "-")
                .setRecordSerializer(KafkaRecordSerializationSchema.<OutputRecord>builder()
                        .setTopicSelector(OutputRecord::topic)
                        .setKeySerializationSchema(keySerializer)
                        .setValueSerializationSchema(valueSerializer)
                        .build())
                .build();

        var lane = execution.fromSource(source, WatermarkStrategy.noWatermarks(), "ordered-cdc-capture")
                .keyBy(ignored -> databaseId + "/" + incarnation + "/" + scope);
        var planned = lane.process(new V2TransactionProjectorFunction(databaseId, incarnation, scope));
        planned
                .name("assemble-project-version")
                .uid("transaction-projector-v2")
                .sinkTo(sink)
                .name("immutable-transaction-protocol");
        execution.execute("transaction-preserving-cdc-projector");
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
