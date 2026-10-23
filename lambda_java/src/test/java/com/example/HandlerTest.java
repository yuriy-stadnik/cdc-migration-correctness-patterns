package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HandlerTest {
    @Test void decodesAwsMskBase64Payload() {
        String json = "{\"protocolVersion\":2}";
        assertEquals(json, Handler.decodeValue(Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8))));
    }
    @Test void resolvesTopicFromRecordBeforeMapKey() {
        KafkaEvent.KafkaEventRecord record = new KafkaEvent.KafkaEventRecord();
        record.setTopic("cdc.tx.control.v2");
        assertEquals("cdc.tx.control.v2", Handler.resolveTopic("wrong-0", record));
    }
}
