package com.example.cdc.projector;

import com.example.cdc.protocol.ProtocolV2Codec;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Isolated v2 checkpoint namespace; all capture and baseline inputs share one planning key. */
public final class V2TransactionProjectorFunction extends KeyedProcessFunction<String, CaptureRecord, OutputRecord> {
    private final String database, incarnation, scope;
    private transient ValueState<String> checkpoint;
    private transient V2Projector planner;
    public V2TransactionProjectorFunction(String database, String incarnation, String scope) {
        this.database = database; this.incarnation = incarnation; this.scope = scope;
    }
    @Override public void open(Configuration parameters) {
        if (getRuntimeContext().getNumberOfParallelSubtasks() != 1)
            throw new IllegalArgumentException("v2 requires one verified planning lane");
        checkpoint = getRuntimeContext().getState(new ValueStateDescriptor<>("source-version-projector-v2", String.class));
        planner = new V2Projector(database, incarnation, scope);
    }
    @Override public void processElement(CaptureRecord record, Context context, Collector<OutputRecord> out) throws Exception {
        V2ProjectorState state = checkpoint.value() == null ? new V2ProjectorState()
                : ProtocolV2Codec.mapper().readValue(checkpoint.value(), V2ProjectorState.class);
        for (OutputRecord output : planner.accept(state, record)) out.collect(output);
        checkpoint.update(ProtocolV2Codec.json(state));
    }
}
