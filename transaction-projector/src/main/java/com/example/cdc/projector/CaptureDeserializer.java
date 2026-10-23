package com.example.cdc.projector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

public final class CaptureDeserializer implements KafkaRecordDeserializationSchema<CaptureRecord> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CaptureRecord> out) {
        if (record.value() != null) {
            out.collect(new CaptureRecord(record.topic(), record.partition(), record.offset(), record.timestamp(),
                    new String(record.value(), StandardCharsets.UTF_8)));
        }
    }

    @Override
    public TypeInformation<CaptureRecord> getProducedType() {
        return TypeInformation.of(CaptureRecord.class);
    }
}
