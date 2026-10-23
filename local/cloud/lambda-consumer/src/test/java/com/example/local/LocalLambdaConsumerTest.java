package com.example.local;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LocalLambdaConsumerTest {
    @Test void commitsOnlyTheDurablyStagedPartitionPosition() {
        var offsets = LocalLambdaConsumer.safeOffset("cdc.tx.control.v2", 3, 41);
        assertEquals(1, offsets.size());
        assertEquals(42, offsets.get(new TopicPartition("cdc.tx.control.v2", 3)).offset());
    }
}
