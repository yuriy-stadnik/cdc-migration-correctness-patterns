package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.example.cdc.apply.TransactionApplyService;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HandlerTest {
    record Delivery(String topic, int partition, long offset, String value) {}
    static final class RecordingProcessor implements Handler.Processor {
        final List<Delivery> deliveries = new ArrayList<>();
        int drainResult;
        boolean failStage;

        @Override public TransactionApplyService.StageResult stage(String topic, int partition, long offset, String json) throws SQLException {
            if (failStage) throw new SQLException("database unavailable", "08006");
            deliveries.add(new Delivery(topic, partition, offset, json));
            return TransactionApplyService.StageResult.STAGED;
        }
        @Override public int drain(int maxTransactions) {
            assertEquals(20, maxTransactions);
            return drainResult;
        }
    }

    static KafkaEvent.KafkaEventRecord record(String topic, int partition, long offset, String value) {
        KafkaEvent.KafkaEventRecord record = new KafkaEvent.KafkaEventRecord();
        record.setTopic(topic); record.setPartition(partition); record.setOffset(offset);
        record.setValue(value == null ? null : Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8)));
        return record;
    }

    @Test void decodesAwsMskBase64Payload() {
        String json = "{\"protocolVersion\":2}";
        assertEquals(json, Handler.decodeValue(Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8))));
    }
    @Test void resolvesTopicFromRecordBeforeMapKey() {
        KafkaEvent.KafkaEventRecord record = new KafkaEvent.KafkaEventRecord();
        record.setTopic("cdc.tx.control.v2");
        assertEquals("cdc.tx.control.v2", Handler.resolveTopic("wrong-0", record));
    }

    @Test void stagesEveryNonNullAwsRecordThenDrains() {
        RecordingProcessor processor = new RecordingProcessor(); processor.drainResult = 3;
        KafkaEvent event = new KafkaEvent();
        Map<String,List<KafkaEvent.KafkaEventRecord>> records = new LinkedHashMap<>();
        records.put("cdc.tx.control.v2-2", List.of(record(null,2,40,"control")));
        records.put("cdc.tx.mutations.v2-1", List.of(record("cdc.tx.mutations.v2",1,41,"mutation"),
                record("cdc.tx.mutations.v2",1,42,null)));
        event.setRecords(records);

        Map<String,Object> result = new Handler(processor).handleRequest(event,null);

        assertEquals(Map.of("status","ok","staged",2,"applied",3),result);
        assertEquals(List.of(new Delivery("cdc.tx.control.v2",2,40,"control"),
                new Delivery("cdc.tx.mutations.v2",1,41,"mutation")),processor.deliveries);
    }

    @Test void scheduledInvocationWithoutKafkaRecordsStillDrains() {
        RecordingProcessor processor = new RecordingProcessor(); processor.drainResult = 1;
        assertEquals(Map.of("status","ok","staged",0,"applied",1),new Handler(processor).handleRequest(null,null));
        assertTrue(processor.deliveries.isEmpty());
    }

    @Test void databaseFailureFailsTheInvocationSoMskOffsetsAreNotAcknowledged() {
        RecordingProcessor processor = new RecordingProcessor(); processor.failStage = true;
        KafkaEvent event = new KafkaEvent();
        event.setRecords(Map.of("cdc.tx.control.v2-0",List.of(record(null,0,1,"control"))));
        RuntimeException failure = assertThrows(RuntimeException.class,()->new Handler(processor).handleRequest(event,null));
        assertInstanceOf(SQLException.class,failure.getCause());
    }

    @Test void validatesSecretsManagerPayload() throws Exception {
        assertEquals("correct horse",Handler.passwordFromSecret("{\"username\":\"appuser\",\"password\":\"correct horse\"}"));
        assertThrows(IllegalArgumentException.class,()->Handler.passwordFromSecret("{}"));
        assertThrows(IllegalArgumentException.class,()->Handler.passwordFromSecret("null"));
        assertThrows(IllegalArgumentException.class,()->Handler.passwordFromSecret("{\"password\":123}"));
        assertThrows(IllegalArgumentException.class,()->Handler.passwordFromSecret(" "));
    }
}
