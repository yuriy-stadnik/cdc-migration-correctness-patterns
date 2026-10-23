package com.example.cdc.projector;

import com.example.cdc.protocol.ProtocolCodec;
import com.example.cdc.protocol.Protocol.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ProjectorFailureTest {
    private long offset;
    private KeyedOneInputStreamOperatorTestHarness<String,CaptureRecord,OutputRecord> harness() throws Exception {
        var h=new KeyedOneInputStreamOperatorTestHarness<String,CaptureRecord,OutputRecord>(
                new KeyedProcessOperator<>(new TransactionProjectorFunction("db","inc","scope")),r->"lane",Types.STRING);
        h.open(); return h;
    }
    private void send(KeyedOneInputStreamOperatorTestHarness<String,CaptureRecord,OutputRecord> h,String json)throws Exception {
        h.processElement(new StreamRecord<>(new CaptureRecord("capture",0,offset++,0,json)));
    }
    private void boundary(KeyedOneInputStreamOperatorTestHarness<String,CaptureRecord,OutputRecord> h,String id,String status,int count)throws Exception {
        send(h,"{\"id\":\""+id+":900\",\"status\":\""+status+"\",\"event_count\":"+count+",\"data_collections\":[{\"data_collection\":\"inventory.customers\",\"event_count\":"+count+"}]}");
    }
    private void data(KeyedOneInputStreamOperatorTestHarness<String,CaptureRecord,OutputRecord> h,String id,long seq,long customer)throws Exception {
        send(h,"{\"op\":\"c\",\"source\":{\"schema\":\"inventory\",\"table\":\"customers\",\"lsn\":800},\"transaction\":{\"id\":\""+id+":800\",\"total_order\":"+seq+"},\"after\":{\"id\":"+customer+",\"first_name\":\"A\",\"last_name\":\"B\"}}");
    }
    private static List<OutputRecord> outputs(KeyedOneInputStreamOperatorTestHarness<String,CaptureRecord,OutputRecord> h) {
        return h.extractOutputStreamRecords().stream().<OutputRecord>map(StreamRecord::getValue).toList();
    }
    private static List<ControlMessage> manifests(KeyedOneInputStreamOperatorTestHarness<String,CaptureRecord,OutputRecord> h) {
        return outputs(h).stream().filter(r->r.topic().equals(com.example.cdc.protocol.Protocol.CONTROL_TOPIC))
                .map(r->ProtocolCodec.read(r.value(),ControlMessage.class)).filter(c->c.messageType()==ControlType.MANIFEST).toList();
    }
    @Test void planningFrontierUsesCommitEndOrderRatherThanBeginOrder()throws Exception {
        try(var h=harness()) {
            boundary(h,"9","BEGIN",2);data(h,"9",1,1);
            boundary(h,"10","BEGIN",1);data(h,"10",1,3);boundary(h,"10","END",1);
            assertEquals(List.of("db/inc/scope/10"),manifests(h).stream().map(ControlMessage::transactionId).toList(),
                    "a transaction that began earlier but has not committed cannot block an earlier END");
            data(h,"9",2,2);boundary(h,"9","END",2);
            assertEquals(List.of("db/inc/scope/10","db/inc/scope/9"),manifests(h).stream().map(ControlMessage::transactionId).toList());
        }
    }
    @Test void lateFragmentAfterDlqMustNotProduceManifest()throws Exception {
        try(var h=harness()) {
            boundary(h,"1","BEGIN",2);data(h,"1",1,1);boundary(h,"1","END",2);
            h.setProcessingTime(600_001); assertTrue(outputs(h).stream().anyMatch(r->r.value().contains("INCOMPLETE")));
            h.setProcessingTime(86_400_001); assertTrue(outputs(h).stream().anyMatch(r->r.topic().endsWith("dlq.v1")));
            data(h,"1",2,2);assertTrue(manifests(h).isEmpty(),"DLQ requires audited requeue");
        }
    }
    @Test void timeoutCheckpointRestoreAndLateFragmentBeforeDlqRecover()throws Exception {
        var first=harness();boundary(first,"1","BEGIN",2);data(first,"1",1,1);boundary(first,"1","END",2);
        var snapshot=first.snapshot(1,1); first.close();
        var restored=new KeyedOneInputStreamOperatorTestHarness<String,CaptureRecord,OutputRecord>(
                new KeyedProcessOperator<>(new TransactionProjectorFunction("db","inc","scope")),r->"lane",Types.STRING);
        try(restored) {
            restored.initializeState(snapshot);restored.open();restored.setProcessingTime(600_001);
            data(restored,"1",1,1);assertTrue(manifests(restored).isEmpty());data(restored,"1",2,2);
            assertEquals(1,manifests(restored).size());assertEquals(2,manifests(restored).get(0).capturedSourceEventCount());
            assertEquals("900",manifests(restored).get(0).sourceCommitPosition(),"late data must not replace END position");
        }
    }
    @Test void transactionReplayMustNotGenerateNewVersionsOrHashes()throws Exception {
        try(var h=harness()) {
            boundary(h,"1","BEGIN",1);data(h,"1",1,1);boundary(h,"1","END",1);
            var original=List.copyOf(outputs(h));h.getOutput().clear();h.setProcessingTime(1000);
            boundary(h,"1","BEGIN",1);data(h,"1",1,1);boundary(h,"1","END",1);
            assertTrue(outputs(h).isEmpty() || outputs(h).equals(original),"replay must suppress duplicates or reproduce immutable output");
        }
    }
    @Test void endCollectionCountsMustMatchCapturedEvents()throws Exception {
        try(var h=harness()) {
            boundary(h,"1","BEGIN",1);data(h,"1",1,1);
            send(h,"{\"id\":\"1:900\",\"status\":\"END\",\"event_count\":1,\"data_collections\":[{\"data_collection\":\"inventory.accounts\",\"event_count\":1}]}");
            assertTrue(manifests(h).isEmpty(),"matching total count does not validate per-collection coverage");
        }
    }
    @Test void conflictingDuplicateFragmentIsQuarantinedWithoutCrashingOperator()throws Exception {
        try(var h=harness()) {
            boundary(h,"1","BEGIN",2);data(h,"1",1,1);
            send(h,"{\"op\":\"c\",\"source\":{\"schema\":\"inventory\",\"table\":\"customers\",\"lsn\":801},\"transaction\":{\"id\":\"1:801\",\"total_order\":1},\"after\":{\"id\":9,\"first_name\":\"Conflict\",\"last_name\":\"B\"}}");
            assertTrue(outputs(h).stream().anyMatch(r->r.topic().endsWith("dlq.v1") && r.value().contains("conflicting duplicate source fragment")));
            data(h,"1",2,2);boundary(h,"1","END",2);
            assertTrue(manifests(h).isEmpty(),"quarantined transaction cannot later produce a manifest");
        }
    }
    @Test void conflictingDuplicateEndMetadataIsQuarantined()throws Exception {
        try(var h=harness()) {
            boundary(h,"1","BEGIN",2);data(h,"1",1,1);boundary(h,"1","END",2);
            boundary(h,"1","END",3);
            assertTrue(outputs(h).stream().anyMatch(r->r.topic().endsWith("dlq.v1") && r.value().contains("conflicting duplicate END")));
            data(h,"1",2,2);assertTrue(manifests(h).isEmpty());
        }
    }
}
